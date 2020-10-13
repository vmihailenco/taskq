package taskq

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHasFreeSystemResources(t *testing.T) {
	// TODO: Manage to mock capnm/sysinfo
	assert.True(t, hasFreeSystemResources(SystemResources{}))
}
