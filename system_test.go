package taskq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDefaultSystemResources(t *testing.T) {
	expectedValue := SystemResources{
		Load1PerCPU:          1.5,
		MemoryFreeMB:         2e5,
		MemoryFreePercentage: 5,
	}
	assert.Equal(t, expectedValue, NewDefaultSystemResources())
}
