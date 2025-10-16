package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ukpabik/HermesMQ/internal/broker"
	"github.com/ukpabik/HermesMQ/internal/redis"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM, os.Interrupt)

	go func() {
		sig := <-sigChan
		log.Printf("received signal: %v — initiating shutdown...", sig)
		cancel()
	}()

	redis.InitializeRedisClient("localhost:6379")

	b := broker.InitializeBroker(":8080")
	log.Printf("Starting server on port %s", b.Port)
	if err := b.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("broker error: %v", err)
	}

	log.Println("broker stopped gracefully ✨")
	time.Sleep(500 * time.Millisecond)
}
