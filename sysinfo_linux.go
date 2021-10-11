//go:build linux
// +build linux

package taskq

import (
	"runtime"

	"github.com/capnm/sysinfo"
)

func hasFreeSystemResources() bool {
	si := sysinfo.Get()
	free := si.FreeRam + si.BufferRam

	// at least 200MB of RAM is free
	if free < 2e5 {
		return false
	}

	// at least 5% of RAM is free
	if float64(free)/float64(si.TotalRam) < 0.05 {
		return false
	}

	// avg load is not too high
	if si.Loads[0] > 1.5*float64(runtime.NumCPU()) {
		return false
	}

	return true
}
