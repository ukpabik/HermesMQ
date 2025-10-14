package protocol

import "time"

type Payload struct {
	Action    string      `json:"action"`
	Type      string      `json:"type"`
	Topic     string      `json:"topic"`
	Body      interface{} `json:"body"`
	Timestamp time.Time   `json:"timestamp"`
	SenderID  string      `json:"sender_id"`
}

type Type string

const (
	Data  Type = "data"
	Error Type = "error"
	ACK   Type = "ack"
)
