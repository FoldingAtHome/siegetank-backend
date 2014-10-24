/* Implementation of a Priority Queue */
package scv

import (
	"container/heap"
)

type QueueItem struct {
	value *Stream
	index int
}

type PriorityQueue []*QueueItem

func (pq PriorityQueue) Len() int {
	return len(pq)
}

func (pq PriorityQueue) Less(i, j int) bool {
	return pq[i].entireFrames > pq[j].entireFrames
}

func (pq PriorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *PriorityQueue) Push(x interface{}) {
	n := len(*pq)
	item := x.(*QueueItem)
	item.index = n
	*pq = append(*pq, item)
}

func (pq *PriorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	*pq = old[0 : n-1]
	return item
}

// // update modifies the priority and value of an QueueItem in the queue.
// func (pq *PriorityQueue) update(item *QueueItem, value string, priority int) {
// 	item.value = value
// 	item.entireFrames = priority
// 	heap.Fix(pq, item.index)
// }
