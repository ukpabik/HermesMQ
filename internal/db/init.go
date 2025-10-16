package db

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ukpabik/HermesMQ/internal/shared"
)

func InitializeClickHouseClient(addr string, port int) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", addr, port)},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
			Password: "",
		},
		Debugf: func(format string, v ...interface{}) {
			log.Printf(format, v...)
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
		DialTimeout:      time.Second * 30,
		MaxOpenConns:     10,
		MaxIdleConns:     5,
		ConnMaxLifetime:  time.Duration(10) * time.Minute,
		ConnOpenStrategy: clickhouse.ConnOpenInOrder,
	})

	if err != nil {
		log.Printf("unable to initialize connection to ClickHouse: %v", err)
		return
	}

	// Test the connection
	ctx := context.Background()
	if err := conn.Ping(ctx); err != nil {
		log.Printf("unable to ping ClickHouse: %v", err)
		return
	}

	log.Println("Connected to ClickHouse client")
	shared.ClickHouseClient = &conn
}

func CreateTables() error {
	clickhouseClient := *shared.ClickHouseClient
	if clickhouseClient == nil {
		return fmt.Errorf("clickhouse client not initialized")
	}

	ctx := context.Background()

	err := clickhouseClient.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS messages (
            id UUID DEFAULT generateUUIDv4(),
            topic String,
            body String,
						sender_id String,
            created_at DateTime DEFAULT now(),
        ) ENGINE = MergeTree()
        ORDER BY (topic, created_at)
        PARTITION BY toYYYYMM(created_at)
    `)

	if err != nil {
		return fmt.Errorf("failed to create messages table: %w", err)
	}

	log.Println("Tables created successfully")
	return nil
}
