// +build !linux

package taskq

func hasFreeSystemResources(_ SystemResources) bool {
	return true
}
