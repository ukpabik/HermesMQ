package redis

import (
	go_redis "github.com/redis/go-redis/v9"
	"github.com/ukpabik/HermesMQ/internal/shared"
)

func InitializeRedisClient(addr string) {
	client := go_redis.NewClient(&go_redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
		Protocol: 2,
	})

	shared.RedisClient = &shared.RedisHandler{
		Client: client,
	}
}
