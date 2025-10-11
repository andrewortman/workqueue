package workqueue

// delayHeap: min-heap by delayUntil time.
type delayHeap[K comparable, V comparable] []*node[K, V]

func (h delayHeap[K, V]) Len() int { return len(h) }

func (h delayHeap[K, V]) Less(i, j int) bool {
	// Earlier delayUntil comes first. A zero time means no delay, so it's always first.
	if h[i].item.DelayedUntil.IsZero() {
		return true
	}
	if h[j].item.DelayedUntil.IsZero() {
		return false
	}
	return h[i].item.DelayedUntil.Before(h[j].item.DelayedUntil)
}

func (h delayHeap[K, V]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idxDelay = i
	h[j].idxDelay = j
}

func (h *delayHeap[K, V]) Push(x interface{}) {
	n := x.(*node[K, V])
	n.idxDelay = len(*h)
	*h = append(*h, n)
}

func (h *delayHeap[K, V]) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.idxDelay = -1
	*h = old[0 : n-1]
	return item
}
