package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/ukpabik/HermesMQ/internal/protocol"
	"github.com/ukpabik/HermesMQ/internal/shared"
)

func StorePayload(payload protocol.Payload) error {
	redisClient := shared.RedisClient
	if redisClient == nil {
		return fmt.Errorf("redis client not initialized")
	}

	redisClient.Mutex.Lock()
	defer redisClient.Mutex.Unlock()

	key := fmt.Sprintf("msg:%s:%s:%d", payload.Topic, payload.SenderID, payload.Timestamp.UnixNano())
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("unable to marshal data: %v", err)
	}

	ctx := context.Background()
	err = redisClient.Client.Set(ctx, key, data, 24*time.Hour).Err()
	if err != nil {
		return fmt.Errorf("error storing to redis: %v", err)
	}

	log.Printf("✅ stored message to redis: %s", key)
	return nil
}

func GetPayload(key string) (*protocol.Payload, error) {
	redisClient := shared.RedisClient
	if redisClient == nil {
		return nil, fmt.Errorf("redis client not initialized")
	}

	redisClient.Mutex.Lock()
	defer redisClient.Mutex.Unlock()

	ctx := context.Background()
	data, err := redisClient.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return nil, fmt.Errorf("message not found")
	}
	if err != nil {
		return nil, fmt.Errorf("error retrieving from redis: %v", err)
	}

	var payload protocol.Payload
	err = json.Unmarshal([]byte(data), &payload)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling: %v", err)
	}

	return &payload, nil
}

func StoreOffset(topic, clientID string, offset int64) error {
	redisClient := shared.RedisClient
	if redisClient == nil {
		return fmt.Errorf("redis client not initialized")
	}

	redisClient.Mutex.Lock()
	defer redisClient.Mutex.Unlock()

	ctx := context.Background()
	key := fmt.Sprintf("offset:%s:%s", topic, clientID)

	err := redisClient.Client.Set(ctx, key, offset, 0).Err()
	if err != nil {
		return fmt.Errorf("unable to store offset: %v", err)
	}

	log.Printf("✅ stored offset for %s/%s: %d", topic, clientID, offset)
	return nil
}

func GetOffset(topic, clientID string) (int64, error) {
	redisClient := shared.RedisClient
	if redisClient == nil {
		return 0, fmt.Errorf("redis client not initialized")
	}

	redisClient.Mutex.Lock()
	defer redisClient.Mutex.Unlock()

	ctx := context.Background()
	key := fmt.Sprintf("offset:%s:%s", topic, clientID)

	val, err := redisClient.Client.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("error retrieving offset: %v", err)
	}

	var offset int64
	offset, err = strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("error parsing offset: %v", err)
	}

	return offset, nil
}
