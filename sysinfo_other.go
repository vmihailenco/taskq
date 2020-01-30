// +build !linux

package taskq

func hasFreeSystemResources() bool {
	return true
}
