package shared

import (
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/redis/go-redis/v9"
)

type RedisHandler struct {
	Client *redis.Client
	Mutex  sync.Mutex
}

var RedisClient *RedisHandler
var ClickHouseClient *driver.Conn
