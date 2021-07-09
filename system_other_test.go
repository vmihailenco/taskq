package taskq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasFreeSystemResources(t *testing.T) {
	assert.True(t, hasFreeSystemResources(SystemResources{}))
}
