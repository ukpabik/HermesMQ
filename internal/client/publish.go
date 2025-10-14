package client

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ukpabik/HermesMQ/internal/protocol"
)

func (cl *Client) Publish(topicName string, body interface{}) error {
	if cl == nil || cl.Connection == nil {
		return fmt.Errorf("client or connection is nil")
	}
	ctx := context.Background()

	_, _, reset, ok, err := cl.PublishStore.Take(ctx, cl.ID)
	if err != nil || !ok {
		resetTime := time.Unix(0, int64(reset))
		waitDuration := time.Until(resetTime)
		if waitDuration < 0 {
			waitDuration = 0
		}
		return fmt.Errorf("publish rate limited, retry in %v", waitDuration.Round(time.Millisecond))
	}

	topicName = strings.TrimSpace(topicName)
	if topicName == "" {
		return fmt.Errorf("topic name cannot be empty")
	}
	if body == nil {
		return fmt.Errorf("message body cannot be nil")
	}

	payload := &protocol.Payload{
		Action:    "publish",
		Topic:     topicName,
		Body:      body,
		Timestamp: time.Now().UTC(),
		SenderID:  cl.ID,
	}

	if err := sendBytes(*payload, cl); err != nil {
		return fmt.Errorf("failed to publish to topic %s: %w", topicName, err)
	}
	return nil
}
