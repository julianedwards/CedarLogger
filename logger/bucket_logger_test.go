package logger

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBucketLoggerImplementation(t *testing.T) {
	assert.Implements(t, (*ReadCloser)(nil), &bucketReader{})
	assert.Implements(t, (*Logger)(nil), &bucketLogger{})
}
