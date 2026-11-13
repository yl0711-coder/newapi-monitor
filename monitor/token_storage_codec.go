package monitor

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"reflect"

	"gorm.io/gorm/schema"
)

const tokenStorageHeader = "TKJ1\x00"
const tokenStorageDecodedLimit = 1 << 20

// Only new, explicitly separated Token stores opt into compressed writes.
// Go values, decision hashes, exports and logical payload budgets remain the
// exact original JSON. Old plain-text rows remain readable without rewriting.
type tokenStorageJSON struct{}

func init() { schema.RegisterSerializer("token_storage_json_v1", tokenStorageJSON{}) }

func tokenStorageEncode(raw string) ([]byte, error) {
	if len(raw) > tokenStorageDecodedLimit {
		return nil, errors.New("token stored JSON exceeds limit")
	}
	var body bytes.Buffer
	body.WriteString(tokenStorageHeader)
	w, err := gzip.NewWriterLevel(&body, gzip.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := io.WriteString(w, raw); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return body.Bytes(), nil
}

func tokenStorageDecode(raw []byte) (string, bool, error) {
	compressed := bytes.HasPrefix(raw, []byte(tokenStorageHeader))
	if !compressed {
		if len(raw) > tokenStorageDecodedLimit {
			return "", false, errors.New("token stored JSON exceeds limit")
		}
		return string(raw), false, nil
	}
	if len(raw) > tokenStorageDecodedLimit+128 {
		return "", true, errors.New("token compressed JSON exceeds limit")
	}
	reader := bytes.NewReader(raw[len(tokenStorageHeader):])
	z, err := gzip.NewReader(reader)
	if err != nil {
		return "", true, errors.New("token stored JSON is invalid")
	}
	defer z.Close()
	z.Multistream(false)
	decoded, err := io.ReadAll(io.LimitReader(z, tokenStorageDecodedLimit+1))
	if err != nil || len(decoded) > tokenStorageDecodedLimit || reader.Len() != 0 {
		return "", true, errors.New("token stored JSON is invalid")
	}
	return string(decoded), true, nil
}

func (tokenStorageJSON) Scan(ctx context.Context, field *schema.Field, dst reflect.Value, value any) error {
	var raw []byte
	switch v := value.(type) {
	case nil:
	case string:
		raw = []byte(v)
	case []byte:
		raw = v
	default:
		return errors.New("token stored JSON type is invalid")
	}
	decoded, compact, err := tokenStorageDecode(raw)
	if err != nil {
		return err
	}
	if flag := field.Schema.FieldsByName["CompactStorage"]; flag != nil {
		if err := flag.Set(ctx, dst, compact); err != nil {
			return err
		}
	}
	return field.Set(ctx, dst, decoded)
}

func (tokenStorageJSON) Value(ctx context.Context, field *schema.Field, dst reflect.Value, value any) (any, error) {
	raw, ok := value.(string)
	if !ok {
		return nil, errors.New("token stored JSON type is invalid")
	}
	compact := false
	if flag := field.Schema.FieldsByName["CompactStorage"]; flag != nil {
		v, _ := flag.ValueOf(ctx, dst)
		compact, _ = v.(bool)
	}
	if !compact || len(raw) < 512 {
		return raw, nil
	}
	encoded, err := tokenStorageEncode(raw)
	if err != nil {
		return nil, err
	}
	if len(encoded) >= len(raw) {
		return raw, nil
	}
	return encoded, nil
}
