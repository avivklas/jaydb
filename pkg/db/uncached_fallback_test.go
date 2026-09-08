package db

import (
	"context"
	"testing"
)

// A read that falls through to local storage because the owning node did not
// answer must NOT be cached, and must not be served from a cache on a later
// call.
//
// The failure this pins down is not a slow read, it is a WRONG one. A non-owner
// that caches a key holds an entry no write can reach: writes for that key are
// routed to the owner, and the owner's Put invalidates only the owner's cache.
// With a namespace TTL measured in hours the non-owner then hands out an ETag
// that cold storage has long since moved past. The client reads it, returns it
// as If-Match, and every conditional write fails with 412 while the validator it
// was handed looks entirely legitimate.
func TestNonOwnerFallbackReadIsNotCached(t *testing.T) {
	d, drv, ring, self := newClusteredDB(t)
	ctx := context.Background()

	keys := testKeys(24)

	// Add a peer that owns some of the keys. Nothing listens on that address, so
	// the inter-query hop fails and Get takes the local-read fall-through -- the
	// exact path under test.
	peer := pickPeer(t, []string{self}, keys)
	ring.AddNode(peer)

	moved, _ := splitByOwner(t, ring, self, keys)
	key := moved[0]

	// Seed cold storage directly. Going through cacheMgr.Put would admit the
	// object locally and pre-empt the very behaviour being tested.
	first, err := drv.Put(ctx, key, []byte(`{"v":1}`), "")
	if err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}

	got, err := d.Get(ctx, key, nil)
	if err != nil {
		t.Fatalf("first get of peer-owned key %s: %v", key, err)
	}
	if got.ETag != first.ETag {
		t.Fatalf("first get ETag = %q, want %q", got.ETag, first.ETag)
	}

	// Move the document out from under the reader, as the owning node's write
	// would. Written straight to cold storage, so nothing invalidates any cache
	// this node might wrongly be holding.
	second, err := drv.Put(ctx, key, []byte(`{"v":2}`), "")
	if err != nil {
		t.Fatalf("overwrite %s: %v", key, err)
	}
	if second.ETag == first.ETag {
		t.Fatal("driver returned the same ETag for different content: the test cannot detect staleness")
	}

	readsBefore := drv.getCalls.Load()

	got, err = d.Get(ctx, key, nil)
	if err != nil {
		t.Fatalf("second get of peer-owned key %s: %v", key, err)
	}

	if got.ETag == first.ETag {
		t.Fatalf("second get served the superseded ETag %q: a non-owner cached the key, "+
			"so every conditional write from this reader would fail with 412", got.ETag)
	}
	if got.ETag != second.ETag {
		t.Fatalf("second get ETag = %q, want the current %q", got.ETag, second.ETag)
	}
	if drv.getCalls.Load() == readsBefore {
		t.Fatal("second get answered without touching cold storage: it came from a cache " +
			"this node has no right to populate for a key it does not own")
	}
}

// The owner's own reads must still be cached: ownership routing is what makes a
// generous namespace TTL safe, and removing the cache for owned keys would turn
// every read into a cold-storage round-trip.
func TestOwnedReadStillUsesCache(t *testing.T) {
	d, drv, ring, self := newClusteredDB(t)
	ctx := context.Background()

	keys := testKeys(24)
	peer := pickPeer(t, []string{self}, keys)
	ring.AddNode(peer)

	_, retained := splitByOwner(t, ring, self, keys)
	key := retained[0]

	if _, err := drv.Put(ctx, key, []byte(`{"v":1}`), ""); err != nil {
		t.Fatalf("seed %s: %v", key, err)
	}

	if _, err := d.Get(ctx, key, nil); err != nil {
		t.Fatalf("first get of own key %s: %v", key, err)
	}

	readsBefore := drv.getCalls.Load()
	if _, err := d.Get(ctx, key, nil); err != nil {
		t.Fatalf("second get of own key %s: %v", key, err)
	}

	if drv.getCalls.Load() != readsBefore {
		t.Fatal("second get of an OWNED key hit cold storage: the owner's cache is what " +
			"makes ownership routing worth having")
	}
}
