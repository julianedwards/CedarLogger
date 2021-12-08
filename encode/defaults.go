package encode

import (
	"bytes"
	"encoding/gob"
	"encoding/json"

	"github.com/pkg/errors"
)

const TEXT = "plain_text"

type textEncoding struct{}

func (e *textEncoding) String() string    { return TEXT }
func (e *textEncoding) Extension() string { return "txt" }
func (e *textEncoding) Marshal(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(v); err != nil {
		return nil, errors.WithStack(err)
	}

	return buf.Bytes(), nil
}

func (e *textEncoding) Unmarshal(data []byte, v interface{}) error {
	switch s := v.(type) {
	case *string:
		*s = string(data)
	default:
		return errors.Errorf("cannot unmarshal plain text to type '%T'", s)

	}

	return nil
}

const JSON = "json"

type jsonEncoding struct{}

func (e *jsonEncoding) String() string    { return JSON }
func (e *jsonEncoding) Extension() string { return JSON }
func (e *jsonEncoding) Marshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (e *jsonEncoding) Unmarshal(data []byte, v interface{}) error {
	return json.Unmarshal(data, v)
}
