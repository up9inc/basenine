package basenine

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckError(t *testing.T) {
	assert.Panics(t, assert.PanicTestFunc(func() {
		Check(errors.New("something"))
	}))

	assert.NotPanics(t, assert.PanicTestFunc(func() {
		Check(nil)
	}))
}
