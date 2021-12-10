package options

import "github.com/pkg/errors"

type Read struct {
	Key      string
	Metadata bool
}

func (o Read) Validate() error {
	if o.Key == "" {
		return errors.New("must specify a key")
	}

	return nil
}
