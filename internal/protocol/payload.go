package protocol

import "time"

type Payload struct {
	Action    string      `json:"action"`
	Topic     string      `json:"topic"`
	Body      interface{} `json:"body"`
	Timestamp time.Time   `json:"timestamp"`
}
