package shared

import (
	"sync"

	"github.com/redis/go-redis/v9"
)

type RedisHandler struct {
	Client *redis.Client
	Mutex  sync.Mutex
}

var RedisClient *RedisHandler
