package protocol

import "time"

type Payload struct {
	Action    string    `json:"action"`
	Topic     string    `json:"topic"`
	Body      string    `json:"body"`
	Timestamp time.Time `json:"timestamp"`
}
