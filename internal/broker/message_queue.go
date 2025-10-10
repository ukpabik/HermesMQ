package broker

import (
	"container/heap"

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
}

func NewPriorityMessageQueue() *PriorityMessageQueue {
	pmq := &PriorityMessageQueue{
		MessageQueue: make(PriorityQueue, 0),
	}
	heap.Init(&pmq.MessageQueue)
	return pmq
}

func (pmq *PriorityMessageQueue) Enqueue(payload *protocol.Payload) {
	heap.Push(&pmq.MessageQueue, payload)
}

func (pmq *PriorityMessageQueue) Dequeue() (*protocol.Payload, bool) {
	if pmq.Size() == 0 {
		return nil, false
	}
	payload := heap.Pop(&pmq.MessageQueue).(*protocol.Payload)
	return payload, true
}

func (pmq *PriorityMessageQueue) Peek() (*protocol.Payload, bool) {
	if pmq.Size() == 0 {
		return nil, false
	}
	return pmq.MessageQueue[0], true
}

func (pmq *PriorityMessageQueue) Size() int {
	return pmq.MessageQueue.Len()
}
