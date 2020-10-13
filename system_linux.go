// +build linux

package taskq

import (
	"runtime"

	"github.com/capnm/sysinfo"
	"github.com/vmihailenco/taskq/v3/internal"
)

func hasFreeSystemResources(sr SystemResources) bool {
	si := sysinfo.Get()
	free := si.FreeRam + si.BufferRam

	if sr.Load1PerCPU > 0 && si.Loads[0] > sr.Load1PerCPU*float64(runtime.NumCPU()) {
		internal.Logger.Println("taskq: consumer memory is lower than required")
		return false
	}

	if sr.MemoryFreeMB > 0 && free < sr.MemoryFreeMB {
		internal.Logger.Println("taskq: consumer memory is lower than required")
		return false
	}

	if sr.MemoryFreePercentage > 0 && free/si.TotalRam < sr.MemoryFreePercentage/100 {
		internal.Logger.Println("taskq: consumer memory is lower than required")
		return false
	}

	return true
}
