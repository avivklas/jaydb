package encoding

import (
	"reflect"
	"testing"
)

type sampleDoc struct {
	Title string `json:"title"`
	Age   int    `json:"age"`
}

func TestJSONCodec(t *testing.T) {
	codec := NewJSONCodec()
	if codec.Name() != "json" {
		t.Fatalf("expected codec name 'json', got '%s'", codec.Name())
	}

	doc := sampleDoc{Title: "Alice", Age: 30}
	data, err := codec.Marshal(doc)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var decoded sampleDoc
	if err := codec.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if !reflect.DeepEqual(doc, decoded) {
		t.Fatalf("expected %+v, got %+v", doc, decoded)
	}
}

func TestRawCodec(t *testing.T) {
	codec := NewRawCodec()
	if codec.Name() != "raw" {
		t.Fatalf("expected codec name 'raw', got '%s'", codec.Name())
	}

	input := []byte("hello world")
	data, err := codec.Marshal(input)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	var out []byte
	if err := codec.Unmarshal(data, &out); err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if string(out) != string(input) {
		t.Fatalf("expected '%s', got '%s'", input, out)
	}
}
