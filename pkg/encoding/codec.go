package encoding

import (
	"encoding/json"
	"fmt"
)

// Codec defines the document serialization interface.
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
	Name() string
}

// JSONCodec is the default document encoder.
type JSONCodec struct{}

func NewJSONCodec() Codec {
	return &JSONCodec{}
}

func (j *JSONCodec) Name() string {
	return "json"
}

func (j *JSONCodec) Marshal(v any) ([]byte, error) {
	if b, ok := v.([]byte); ok {
		return b, nil
	}
	if s, ok := v.(string); ok {
		return []byte(s), nil
	}
	return json.Marshal(v)
}

func (j *JSONCodec) Unmarshal(data []byte, v any) error {
	if dest, ok := v.(*[]byte); ok {
		*dest = append([]byte(nil), data...)
		return nil
	}
	if dest, ok := v.(*string); ok {
		*dest = string(data)
		return nil
	}
	return json.Unmarshal(data, v)
}

// RawCodec handles raw byte slice data without serialization.
type RawCodec struct{}

func NewRawCodec() Codec {
	return &RawCodec{}
}

func (r *RawCodec) Name() string {
	return "raw"
}

func (r *RawCodec) Marshal(v any) ([]byte, error) {
	switch val := v.(type) {
	case []byte:
		return val, nil
	case string:
		return []byte(val), nil
	default:
		return nil, fmt.Errorf("raw codec requires []byte or string, got %T", v)
	}
}

func (r *RawCodec) Unmarshal(data []byte, v any) error {
	switch dest := v.(type) {
	case *[]byte:
		*dest = append([]byte(nil), data...)
		return nil
	case *string:
		*dest = string(data)
		return nil
	default:
		return fmt.Errorf("raw codec requires *[]byte or *string, got %T", v)
	}
}
