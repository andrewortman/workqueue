package workqueue

// priorityHeap: max-heap by priority (higher first), tie-break by id ascending.
type priorityHeap[K comparable, V comparable] []*node[K, V]

func (h priorityHeap[K, V]) Len() int { return len(h) }

func (h priorityHeap[K, V]) Less(i, j int) bool {
	// Higher priority comes first
	if h[i].item.Priority != h[j].item.Priority {
		return h[i].item.Priority > h[j].item.Priority
	}
	// Tie-break by id ascending (smaller id comes first)
	return h[i].id < h[j].id
}

func (h priorityHeap[K, V]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idxPrio = i
	h[j].idxPrio = j
}

func (h *priorityHeap[K, V]) Push(x interface{}) {
	n := x.(*node[K, V])
	n.idxPrio = len(*h)
	*h = append(*h, n)
}

func (h *priorityHeap[K, V]) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.idxPrio = -1
	*h = old[0 : n-1]
	return item
}
