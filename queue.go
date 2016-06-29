package queue

type Storager interface {
	Exists(key string) bool
}
