package taskq

const (
	defaultSystemResourcesLoad1PerCPU          float64 = 1.5
	defaultSystemResourcesMemoryFreeMB         uint64  = 2e5
	defaultSystemResourcesMemoryFreePercentage uint64  = 5
)

// SystemResources represents system related values
type SystemResources struct {
	// Maximum per CPU load at 1min intervals
	Load1PerCPU float64

	// Minimum free memory required in megabytes
	MemoryFreeMB uint64

	// Minimum free memory required in percentage
	MemoryFreePercentage uint64
}

// NewDefaultSystemResources returns a new SystemResources struct with some default values
func NewDefaultSystemResources() SystemResources {
	return SystemResources{
		Load1PerCPU:          defaultSystemResourcesLoad1PerCPU,
		MemoryFreeMB:         defaultSystemResourcesMemoryFreeMB,
		MemoryFreePercentage: defaultSystemResourcesMemoryFreePercentage,
	}
}
