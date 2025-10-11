package workqueue

// expiryHeap: min-heap by expiry time.
type expiryHeap[K comparable, V comparable] []*node[K, V]

func (h expiryHeap[K, V]) Len() int { return len(h) }

func (h expiryHeap[K, V]) Less(i, j int) bool {
	// Earlier expiry comes first. A zero time means it never expires, so it's always last.
	if h[i].item.ExpiresAt.IsZero() {
		return false
	}
	if h[j].item.ExpiresAt.IsZero() {
		return true
	}
	return h[i].item.ExpiresAt.Before(h[j].item.ExpiresAt)
}

func (h expiryHeap[K, V]) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].idxExp = i
	h[j].idxExp = j
}

func (h *expiryHeap[K, V]) Push(x interface{}) {
	n := x.(*node[K, V])
	n.idxExp = len(*h)
	*h = append(*h, n)
}

func (h *expiryHeap[K, V]) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	item.idxExp = -1
	*h = old[0 : n-1]
	return item
}
