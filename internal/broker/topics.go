package broker

import (
	"sync"

	"github.com/ukpabik/HermesMQ/internal/client"
)

type Topic struct {
	Name        string
	Connections []*client.Client
	Mutex       *sync.Mutex
}
