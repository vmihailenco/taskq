package taskq

import (
	"context"
	"sync"
	"time"

	"github.com/hashicorp/golang-lru/simplelru"
)

type Storage interface {
	Exists(ctx context.Context, key string) bool
}

var _ Storage = (*localStorage)(nil)
var _ Storage = (*redisStorage)(nil)

// LOCAL

type localStorage struct {
	mu           sync.Mutex
	cache        *simplelru.LRU
	uniqueMsgTTL time.Duration // time to live for unique messages
}

func NewLocalStorage(a ...interface{}) Storage {
	uniqueMsgTTL := 24 * time.Hour
	if len(a) > 0 {
		duration, ok := a[0].(time.Duration)
		if ok {
			uniqueMsgTTL = duration
		}
	}
	return &localStorage{
		uniqueMsgTTL: uniqueMsgTTL,
	}
}

func (s *localStorage) Exists(_ context.Context, key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.cache == nil {
		var err error
		s.cache, err = simplelru.NewLRU(128000, nil)
		if err != nil {
			panic(err)
		}
	}

	preVal, ok := s.cache.Get(key)
	if ok {
		return true
	}

	preTime, ok := preVal.(time.Time)
	if ok {
		if time.Since(preTime) < s.uniqueMsgTTL {
			return true
		} else {
			s.cache.Remove(key)
			return false
		}
	}

	s.cache.Add(key, time.Now().Add(s.uniqueMsgTTL))

	return false
}

// REDIS

type redisStorage struct {
	redis        Redis
	uniqueMsgTTL time.Duration // time to live for unique messages
}

func newRedisStorage(redis Redis, uniqueMsgTTL time.Duration) Storage {
	return &redisStorage{
		redis:        redis,
		uniqueMsgTTL: uniqueMsgTTL,
	}
}

func (s *redisStorage) Exists(ctx context.Context, key string) bool {
	val, err := s.redis.SetNX(ctx, key, "", s.uniqueMsgTTL).Result()
	if err != nil {
		return true
	}
	return !val
}
