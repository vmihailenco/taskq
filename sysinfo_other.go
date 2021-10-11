//go:build !linux
// +build !linux

package taskq

func hasFreeSystemResources() bool {
	return true
}
