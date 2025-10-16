package db

import (
	"context"
	"fmt"

	"github.com/ukpabik/HermesMQ/internal/protocol"
	"github.com/ukpabik/HermesMQ/internal/shared"
)

func AddMessageToDB(payload protocol.Payload) error {
	clickhouseClient := *shared.ClickHouseClient
	if clickhouseClient == nil {
		return fmt.Errorf("clickhouse client not initialized")
	}

	ctx := context.Background()
	batch, err := clickhouseClient.PrepareBatch(ctx, `
        INSERT INTO messages (
            topic, body, sender_id, created_at
        ) VALUES (?, ?, ?, ?)
    `)

	if err != nil {
		return fmt.Errorf("unable to prepare batch statement: %v", err)
	}

	err = batch.Append(payload.Topic, payload.Body, payload.SenderID, payload.Timestamp)
	if err != nil {
		return fmt.Errorf("unable to add body to batch: %v", err)
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("unable to execute insert into messages: %v", err)
	}
	return nil
}
