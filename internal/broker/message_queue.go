package broker

import (
	"container/heap"
	"sync"

	"github.com/ukpabik/HermesMQ/internal/protocol"
)

type PriorityQueue []*protocol.Payload

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].Timestamp.Before(pq[j].Timestamp)
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *PriorityQueue) Push(x interface{}) {
	payload := x.(*protocol.Payload)
	*pq = append(*pq, payload)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	payload := old[n-1]
	old[n-1] = nil
	*pq = old[0 : n-1]
	return payload
}

type PriorityMessageQueue struct {
	ReadChannel  chan protocol.Payload
	MessageQueue PriorityQueue
	Mutex        sync.Mutex
	notifyCh     chan struct{}
}

func NewPriorityMessageQueue() *PriorityMessageQueue {
	pmq := &PriorityMessageQueue{
		MessageQueue: make(PriorityQueue, 0),
		ReadChannel:  make(chan protocol.Payload, 1024),
		notifyCh:     make(chan struct{}, 1),
	}
	heap.Init(&pmq.MessageQueue)
	return pmq
}

func (pmq *PriorityMessageQueue) Enqueue(payload *protocol.Payload) {
	pmq.Mutex.Lock()
	heap.Push(&pmq.MessageQueue, payload)
	pmq.Mutex.Unlock()

	select {
	case pmq.notifyCh <- struct{}{}:
	default:
	}
}

func (pmq *PriorityMessageQueue) Dequeue() (*protocol.Payload, bool) {
	pmq.Mutex.Lock()
	defer pmq.Mutex.Unlock()

	if len(pmq.MessageQueue) == 0 {
		return nil, false
	}
	return heap.Pop(&pmq.MessageQueue).(*protocol.Payload), true
}

func (pmq *PriorityMessageQueue) Size() int {
	pmq.Mutex.Lock()
	defer pmq.Mutex.Unlock()
	return len(pmq.MessageQueue)
}

func (pmq *PriorityMessageQueue) Peek() (*protocol.Payload, bool) {
	pmq.Mutex.Lock()
	defer pmq.Mutex.Unlock()

	if len(pmq.MessageQueue) == 0 {
		return nil, false
	}
	return pmq.MessageQueue[0], true
}

func (pmq *PriorityMessageQueue) Notify() <-chan struct{} {
	return pmq.notifyCh
}
