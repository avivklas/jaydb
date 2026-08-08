package sharding

import (
	"sync"
	"testing"
)

func TestRingGenerationOnlyBumpsOnRealChange(t *testing.T) {
	r := NewRing(10, 2)

	if gen := r.Generation(); gen != 0 {
		t.Fatalf("a fresh ring should start at generation 0, got %d", gen)
	}

	r.AddNode("10.0.0.1:8080")
	afterAdd := r.Generation()
	if afterAdd != 1 {
		t.Fatalf("expected generation 1 after the first add, got %d", afterAdd)
	}

	// Re-adding a member moves no ownership, so it must not signal a change -
	// every signal costs each DB in the process a full cache purge.
	r.AddNode("10.0.0.1:8080")
	if gen := r.Generation(); gen != afterAdd {
		t.Fatalf("re-adding an existing node bumped the generation to %d", gen)
	}

	// Removing an absent member likewise changes nothing.
	r.RemoveNode("10.0.0.9:8080")
	if gen := r.Generation(); gen != afterAdd {
		t.Fatalf("removing an absent node bumped the generation to %d", gen)
	}

	r.AddNode("10.0.0.2:8080")
	if gen := r.Generation(); gen != 2 {
		t.Fatalf("expected generation 2 after a second real add, got %d", gen)
	}

	r.RemoveNode("10.0.0.2:8080")
	if gen := r.Generation(); gen != 3 {
		t.Fatalf("expected generation 3 after a real removal, got %d", gen)
	}
}

func TestRingGenerationConcurrentReadsAndChanges(t *testing.T) {
	r := NewRing(10, 2)
	r.AddNode("10.0.0.1:8080")

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				_ = r.Generation()
				_ = r.GetNode("users/1/profile")
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for j := 0; j < 50; j++ {
			r.AddNode("10.0.0.2:8080")
			r.RemoveNode("10.0.0.2:8080")
		}
	}()
	wg.Wait()

	// 50 add/remove pairs on top of the initial add.
	if gen := r.Generation(); gen != 101 {
		t.Fatalf("expected generation 101, got %d", gen)
	}
}
