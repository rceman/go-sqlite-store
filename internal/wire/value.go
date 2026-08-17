package wire

import (
	"errors"
	"fmt"
	"math"

	"github.com/rceman/go-sqlite-store/store"
)

type Kind string

const (
	KindNull    Kind = "null"
	KindInteger Kind = "integer"
	KindReal    Kind = "real"
	KindText    Kind = "text"
	KindBlob    Kind = "blob"
	KindBoolean Kind = "boolean"
)

// Value is the typed JSON representation used by the daemon protocol. Separate
// fields avoid JSON's lossy any-number decoding and preserve BLOB vs TEXT.
type Value struct {
	Kind    Kind    `json:"kind"`
	Integer int64   `json:"integer,omitempty"`
	Real    float64 `json:"real,omitempty"`
	Text    string  `json:"text,omitempty"`
	Blob    []byte  `json:"blob,omitempty"`
	Boolean bool    `json:"boolean,omitempty"`
}

type SQLRequest struct {
	SQL  string  `json:"sql"`
	Args []Value `json:"args,omitempty"`
}

type Statement struct {
	SQL  string  `json:"sql"`
	Args []Value `json:"args,omitempty"`
}

type BatchRequest struct {
	Statements []Statement `json:"statements"`
}

type QueryResult struct {
	Columns []string  `json:"columns"`
	Rows    [][]Value `json:"rows"`
}

func EncodeValue(v any) (Value, error) {
	switch x := v.(type) {
	case nil:
		return Value{Kind: KindNull}, nil
	case bool:
		return Value{Kind: KindBoolean, Boolean: x}, nil
	case int:
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case int8:
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case int16:
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case int32:
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case int64:
		return Value{Kind: KindInteger, Integer: x}, nil
	case uint:
		if uint64(x) > math.MaxInt64 {
			return Value{}, errors.New("uint overflows sqlite INTEGER")
		}
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case uint8:
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case uint16:
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case uint32:
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case uint64:
		if x > math.MaxInt64 {
			return Value{}, errors.New("uint64 overflows sqlite INTEGER")
		}
		return Value{Kind: KindInteger, Integer: int64(x)}, nil
	case float32:
		return Value{Kind: KindReal, Real: float64(x)}, nil
	case float64:
		return Value{Kind: KindReal, Real: x}, nil
	case string:
		return Value{Kind: KindText, Text: x}, nil
	case []byte:
		return Value{Kind: KindBlob, Blob: append([]byte(nil), x...)}, nil
	default:
		return Value{}, fmt.Errorf("unsupported SQL value type %T", v)
	}
}

func DecodeValue(v Value) (any, error) {
	switch v.Kind {
	case KindNull:
		return nil, nil
	case KindInteger:
		return v.Integer, nil
	case KindReal:
		return v.Real, nil
	case KindText:
		return v.Text, nil
	case KindBlob:
		return append([]byte(nil), v.Blob...), nil
	case KindBoolean:
		return v.Boolean, nil
	default:
		return nil, fmt.Errorf("unsupported wire value kind %q", v.Kind)
	}
}

func EncodeArgs(args []any) ([]Value, error) {
	out := make([]Value, len(args))
	for i, arg := range args {
		v, err := EncodeValue(arg)
		if err != nil {
			return nil, fmt.Errorf("arg %d: %w", i+1, err)
		}
		out[i] = v
	}
	return out, nil
}

func DecodeArgs(args []Value) ([]any, error) {
	out := make([]any, len(args))
	for i, arg := range args {
		v, err := DecodeValue(arg)
		if err != nil {
			return nil, fmt.Errorf("arg %d: %w", i+1, err)
		}
		out[i] = v
	}
	return out, nil
}

func EncodeQueryResult(in store.QueryResult) (QueryResult, error) {
	out := QueryResult{Columns: append([]string(nil), in.Columns...), Rows: make([][]Value, len(in.Rows))}
	for i, row := range in.Rows {
		out.Rows[i] = make([]Value, len(row))
		for j, cell := range row {
			v, err := EncodeValue(cell)
			if err != nil {
				return QueryResult{}, fmt.Errorf("row %d column %d: %w", i, j, err)
			}
			out.Rows[i][j] = v
		}
	}
	return out, nil
}

func DecodeQueryResult(in QueryResult) (store.QueryResult, error) {
	out := store.QueryResult{Columns: append([]string(nil), in.Columns...), Rows: make([][]any, len(in.Rows))}
	for i, row := range in.Rows {
		out.Rows[i] = make([]any, len(row))
		for j, cell := range row {
			v, err := DecodeValue(cell)
			if err != nil {
				return store.QueryResult{}, fmt.Errorf("row %d column %d: %w", i, j, err)
			}
			out.Rows[i][j] = v
		}
	}
	return out, nil
}
