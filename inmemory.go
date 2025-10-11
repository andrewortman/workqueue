package workqueue

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

var (
	// ErrInvalidItem is a base error for invalid work items.
	ErrInvalidItem = errors.New("workqueue: invalid item")
	// ErrExpiryBeforeDelay is returned when an item's expiry is before its delay.
	ErrExpiryBeforeDelay = fmt.Errorf("%w: expiry is before delay", ErrInvalidItem)
	// ErrItemExists is returned when an item with the same key already exists.
	ErrItemExists = errors.New("workqueue: item already exists")
	// ErrItemNotFound is returned when an item is not found in the queue.
	ErrItemNotFound = errors.New("workqueue: item not found")
)

// WorkItem represents a unit of work in the queue.
type WorkItem[K comparable, V comparable] struct {
	Key   K
	Value V

	Priority     int64     // zero means no priority differentiation
	ExpiresAt    time.Time // if zero, no expiration
	DelayedUntil time.Time // if zero, effectively no delay
}

// node stored in the heaps.
type node[K comparable, V comparable] struct {
	item WorkItem[K, V]
	id   int64 // auto-incrementing id for tie-breaking

	idxPrio  int // index in the priority heap (-1 if not present)
	idxExp   int // index in the expiry heap (-1 if not present)
	idxDelay int // index in the delay heap (-1 if not present)
}

// TimeProvider is an interface for getting the current time.
type TimeProvider interface {
	Now() time.Time
}

// takeRequest is a struct that holds information about a pending Take or TakeMany call.
type takeRequest struct {
	// n is the number of items to take.
	n int
}

// InMemory is an in-memory priority/delay/expiry work queue.
type InMemory[K comparable, V comparable] struct {
	mu   sync.Mutex
	cond *sync.Cond

	now func() time.Time // injectable clock for tests; default time.Now

	prio  priorityHeap[K, V]
	exp   expiryHeap[K, V]
	delay delayHeap[K, V]

	// key -> node
	items map[K]*node[K, V]

	// auto-incrementing id for tie-breaking
	nextID int64

	// timer for delayed items
	timer *time.Timer

	// takeQueue is a queue of pending Take requests, ensuring FIFO processing of takes.
	takeQueue []*takeRequest

	// onTakeWait is a test hook that is called when a consumer starts waiting.
	onTakeWait func()
}

// NewInMemory constructs the queue.
// If timeProvider is nil, time.Now will be used.
func NewInMemory[K comparable, V comparable](timeProvider TimeProvider) *InMemory[K, V] {
	nowFunc := time.Now
	if timeProvider != nil {
		nowFunc = timeProvider.Now
	}

	q := &InMemory[K, V]{
		now:   nowFunc,
		items: make(map[K]*node[K, V]),
	}
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Put synchronously adds new items to the queue.
// Returns an error if an item with the same key already exists.
func (q *InMemory[K, V]) Put(ctx context.Context, items ...WorkItem[K, V]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Err(); err != nil {
		return err
	}

	// pre-flight checks before mutating state
	for _, item := range items {
		// Validate expiry > delayUntil if delayUntil is set
		if err := validateExpiryDelay(item.ExpiresAt, item.DelayedUntil); err != nil {
			return err
		}
		if _, exists := q.items[item.Key]; exists {
			return fmt.Errorf("%w: key %v", ErrItemExists, item.Key)
		}
	}

	q.maintainHeapsLocked()

	for _, item := range items {
		// Push new node
		q.pushNewNodeLocked(item)
	}

	// Yield to awaiting Take operations
	q.cond.Broadcast()

	return nil
}

// Update synchronously updates existing items in the queue.
// Returns an error if any item with the given key does not exist.
// Always updates the item with new values, including heap reorganization if needed.
func (q *InMemory[K, V]) Update(ctx context.Context, items ...WorkItem[K, V]) error {
	return q.internalUpdate(ctx, nil, items...)
}

// UpdateConditional synchronously updates existing items in the queue only if the shouldUpdate
// predicate returns true. Returns an error if any item with the given key does not exist.
// The predicate receives a copy of the existing item and the new item, and should return true
// if the update should proceed.
func (q *InMemory[K, V]) UpdateConditional(ctx context.Context, shouldUpdate func(existing WorkItem[K, V], new WorkItem[K, V]) bool, items ...WorkItem[K, V]) error {
	return q.internalUpdate(ctx, shouldUpdate, items...)
}

// internalUpdate implements the core update logic with an optional predicate.
// If shouldUpdate is nil, all updates are performed unconditionally.
// Must be called with the lock NOT held (it will acquire the lock).
func (q *InMemory[K, V]) internalUpdate(ctx context.Context, shouldUpdate func(existing WorkItem[K, V], new WorkItem[K, V]) bool, items ...WorkItem[K, V]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Err(); err != nil {
		return err
	}

	// pre-flight checks before mutating state
	for _, item := range items {
		// Validate expiry > delayUntil if delayUntil is set
		if err := validateExpiryDelay(item.ExpiresAt, item.DelayedUntil); err != nil {
			return err
		}

		// Check if item exists - error if it doesn't
		if _, exists := q.items[item.Key]; !exists {
			return fmt.Errorf("%w: key %v", ErrItemNotFound, item.Key)
		}
	}

	q.maintainHeapsLocked()

	for _, item := range items {
		existingNode := q.items[item.Key]
		// Check the predicate (if provided), passing a copy of the existing item
		if shouldUpdate == nil || shouldUpdate(existingNode.item, item) {
			if q.nodeNeedsUpdateLocked(existingNode, item) {
				q.updateExistingNodeLocked(existingNode, item)
			}
		}
	}

	q.cond.Broadcast()

	return nil
}

// PutOrUpdate synchronously adds new items or updates existing items.
// For items with duplicate keys in the same batch, the last one wins.
func (q *InMemory[K, V]) PutOrUpdate(ctx context.Context, items ...WorkItem[K, V]) error {
	return q.internalPutOrUpdate(ctx, nil, items...)
}

// PutOrUpdateConditional synchronously adds new items or updates existing items conditionally.
// The shouldUpdate predicate receives a pointer to a copy of the existing item (nil if not found)
// and the new item. It should return true if the update should proceed. For new items (existing == nil),
// returning true will insert the item; returning false will skip it.
func (q *InMemory[K, V]) PutOrUpdateConditional(ctx context.Context, shouldUpdate func(existing *WorkItem[K, V], new WorkItem[K, V]) bool, items ...WorkItem[K, V]) error {
	return q.internalPutOrUpdate(ctx, shouldUpdate, items...)
}

// internalPutOrUpdate implements the core put-or-update logic with an optional predicate.
// If shouldUpdate is nil, all inserts and updates are performed unconditionally.
// Must be called with the lock NOT held (it will acquire the lock).
func (q *InMemory[K, V]) internalPutOrUpdate(ctx context.Context, shouldUpdate func(existing *WorkItem[K, V], new WorkItem[K, V]) bool, items ...WorkItem[K, V]) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Err(); err != nil {
		return err
	}

	// pre-flight checks before mutating state
	for _, item := range items {
		if err := validateExpiryDelay(item.ExpiresAt, item.DelayedUntil); err != nil {
			return err
		}
	}

	// Run maintenance
	q.maintainHeapsLocked()

	// Process items in order. If duplicates exist in the batch, later items will
	// simply overwrite the earlier ones.
	for _, item := range items {
		// Check if item already exists
		if existingNode, exists := q.items[item.Key]; exists {
			// Call predicate with a copy of the existing item (if provided)
			if shouldUpdate == nil {
				if q.nodeNeedsUpdateLocked(existingNode, item) {
					// Update existing node with new values
					q.updateExistingNodeLocked(existingNode, item)
				}
			} else {
				// Make a copy to pass to the predicate
				existingCopy := existingNode.item
				if shouldUpdate(&existingCopy, item) {
					if q.nodeNeedsUpdateLocked(existingNode, item) {
						// Update existing node with new values
						q.updateExistingNodeLocked(existingNode, item)
					}
				}
			}
		} else {
			// Call predicate with nil existing item (if provided)
			if shouldUpdate == nil || shouldUpdate(nil, item) {
				// Push new node
				q.pushNewNodeLocked(item)
			}
		}
	}

	// Yield to awaiting Take operations
	q.cond.Broadcast()

	return nil
}

// Take blocks until an item is available or ctx is canceled.
// On cancellation, returns the context error and the zero value of T.
func (q *InMemory[K, V]) Take(ctx context.Context) (WorkItem[K, V], error) {
	items, err := q.TakeMany(ctx, 1)
	if err != nil {
		var zero WorkItem[K, V]
		return zero, err
	}
	return items[0], nil
}

// TakeMany blocks until n items are available or the context is canceled.
// It waits until a full batch of n items is available before taking any,
// ensuring that items remain in the queue and are eligible for updates until
// the entire batch is returned. If the context is canceled, it returns
// immediately with a context error and no items.
func (q *InMemory[K, V]) TakeMany(ctx context.Context, n int) ([]WorkItem[K, V], error) {
	if n <= 0 {
		return nil, nil
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	// Create and enqueue a take request to ensure first-in-first-out
	// fair scheduling for consumers.
	req := &takeRequest{n: n}
	q.takeQueue = append(q.takeQueue, req)

	// Single cancellation monitor goroutine for this TakeMany call
	cancelMonitor := make(chan struct{})
	defer close(cancelMonitor)
	go func() {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			q.cond.Broadcast()
			q.mu.Unlock()
		case <-cancelMonitor:
			return
		}
	}()

	for {
		// If context was canceled, remove the request from the queue and return.
		if err := ctx.Err(); err != nil {
			q.removeTakeRequestLocked(req)
			return nil, err
		}

		q.maintainHeapsLocked()

		// Check if we are the head of the take queue and if there are enough items.
		if len(q.takeQueue) > 0 && q.takeQueue[0] == req && len(q.prio) >= n {
			// Dequeue the request.
			q.takeQueue = q.takeQueue[1:]

			// Fulfill the request.
			result := make([]WorkItem[K, V], n)
			for i := 0; i < n; i++ {
				node := heap.Pop(&q.prio).(*node[K, V])
				q.removeItemLocked(node)
				result[i] = node.item
			}

			// Broadcast to wake up other waiters, as the queue state has changed.
			q.cond.Broadcast()

			return result, nil
		}

		// wait for new items or context cancellation
		if q.onTakeWait != nil {
			q.onTakeWait()
		}
		q.cond.Wait()
	}
}

type SizeResult struct {
	Pending int
	Delayed int
}

// Size runs GC and returns the number of non-expired items (pending, delayed).
func (q *InMemory[K, V]) Size(ctx context.Context) (SizeResult, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Err(); err != nil {
		return SizeResult{}, err
	}

	q.maintainHeapsLocked()

	// total items are len(q.items); delayed items are in the delay heap
	delayed := len(q.delay)
	pending := len(q.items) - delayed
	return SizeResult{Pending: pending, Delayed: delayed}, nil
}

// Remove synchronously removes an item from the queue by its key.
// Returns an error if no item with the given key exists.
// The removed item is returned if found.
func (q *InMemory[K, V]) Remove(ctx context.Context, key K) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	// Check context after acquiring lock
	if err := ctx.Err(); err != nil {
		return err
	}

	// Run maintenance
	q.maintainHeapsLocked()

	// Check if item exists
	existingNode, exists := q.items[key]
	if !exists {
		return fmt.Errorf("%w: key %v", ErrItemNotFound, key)
	}

	// Remove from all heaps and map
	q.removeItemLocked(existingNode)

	// Yield to awaiting Take operations
	q.cond.Broadcast()

	return nil
}

// removeTakeRequestLocked removes a takeRequest from the takeQueue.
// This is used when a Take/TakeMany call is canceled.
func (q *InMemory[K, V]) removeTakeRequestLocked(req *takeRequest) {
	for i, r := range q.takeQueue {
		if r == req {
			q.takeQueue = append(q.takeQueue[:i], q.takeQueue[i+1:]...)
			break
		}
	}
}

// validateExpiryDelay returns an error if expiry is not after delayUntil when delayUntil is set.
func validateExpiryDelay(expiry time.Time, delayUntil time.Time) error {
	if !delayUntil.IsZero() && !expiry.IsZero() && !expiry.After(delayUntil) {
		return ErrExpiryBeforeDelay
	}
	return nil
}

// rearmDelayTimerLocked (re)arms the delay timer for the earliest delayed item.
// Must be called with mu held.
func (q *InMemory[K, V]) rearmDelayTimerLocked() {
	// Stop any existing timer
	if q.timer != nil {
		q.timer.Stop()
		q.timer = nil
	}

	if len(q.delay) == 0 {
		return
	}

	now := q.now()
	duration := q.delay[0].item.DelayedUntil.Sub(now)
	if duration < 0 {
		duration = 0
	}

	q.timer = time.AfterFunc(duration, func() {
		q.mu.Lock()
		defer q.mu.Unlock()

		// Promote any due delayed items and remove expired ones
		q.maintainHeapsLocked()

		// Wake waiters so Take can proceed without waiting for a producer
		q.cond.Broadcast()
	})
}

// removeNodeFromHeapsLocked removes the node from all heaps it may be in (priority, expiry, delay).
// Must be called with mu held.
func (q *InMemory[K, V]) removeNodeFromHeapsLocked(n *node[K, V]) {
	if n.idxDelay >= 0 {
		heap.Remove(&q.delay, n.idxDelay)
	}
	if n.idxExp >= 0 {
		heap.Remove(&q.exp, n.idxExp)
	}
	if n.idxPrio >= 0 {
		heap.Remove(&q.prio, n.idxPrio)
	}

	// delay heap head may have changed
	q.rearmDelayTimerLocked()
}

// removeItemLocked removes a node from all heaps and the items map.
// Must be called with mu held.
func (q *InMemory[K, V]) removeItemLocked(n *node[K, V]) {
	// Remove from all heaps if present
	q.removeNodeFromHeapsLocked(n)

	// Remove from items map
	delete(q.items, n.item.Key)
}

// moveNodeToDelayHeap moves a node from main heaps to the delay heap.
// Must be called with mu held.
func (q *InMemory[K, V]) moveNodeToDelayHeapLocked(n *node[K, V]) {
	if n.idxPrio >= 0 {
		heap.Remove(&q.prio, n.idxPrio)
	}
	if n.idxExp >= 0 {
		heap.Remove(&q.exp, n.idxExp)
	}
	heap.Push(&q.delay, n)

	// entering delay heap may affect timer
	q.rearmDelayTimerLocked()
}

// moveNodeFromDelayToMainHeaps moves a node from delay heap to main heaps.
// Must be called with mu held.
func (q *InMemory[K, V]) moveNodeFromDelayToMainHeapsLocked(n *node[K, V]) {
	if n.idxDelay >= 0 {
		heap.Remove(&q.delay, n.idxDelay)
	}
	heap.Push(&q.prio, n)
	if !n.item.ExpiresAt.IsZero() {
		heap.Push(&q.exp, n)
	}

	// leaving delay heap may affect the head
	q.rearmDelayTimerLocked()
}

// delayChanged returns true if the delay time has changed between old and new values.
func delayChanged(oldDelay, newDelay time.Time) bool {
	return !oldDelay.Equal(newDelay)
}

// expiryChanged returns true if the expiry time has changed between old and new values.
func expiryChanged(oldExpiry, newExpiry time.Time) bool {
	return !oldExpiry.Equal(newExpiry)
}

// priorityChanged returns true if the priority has changed between old and new values.
func priorityChanged(oldPriority, newPriority int64) bool {
	return oldPriority != newPriority
}

// nodeNeedsUpdate checks if any of the node's properties need to be updated.
// Must be called with mu held.
func (q *InMemory[K, V]) nodeNeedsUpdateLocked(existingNode *node[K, V], item WorkItem[K, V]) bool {
	// Check if value changed
	if !reflect.DeepEqual(existingNode.item.Value, item.Value) {
		return true
	}

	// Check if priority changed
	if priorityChanged(existingNode.item.Priority, item.Priority) {
		return true
	}

	// Check if delay changed
	if delayChanged(existingNode.item.DelayedUntil, item.DelayedUntil) {
		return true
	}

	// Check if expiry changed
	if expiryChanged(existingNode.item.ExpiresAt, item.ExpiresAt) {
		return true
	}

	return false
}

// maintainHeapsLocked promotes due delayed items into the active heaps and removes expired items.
func (q *InMemory[K, V]) maintainHeapsLocked() {
	q.promoteDelayedNodesLocked()
	q.removeExpiredNodesLocked()
	q.rearmDelayTimerLocked()
}

// promoteDelayedNodesLocked promotes all delayed items that are ready (delayUntil <= now) into active heaps.
func (q *InMemory[K, V]) promoteDelayedNodesLocked() {
	now := q.now()

	// Promote all delayed items that are ready into active heaps
	for len(q.delay) > 0 {
		if q.delay[0].item.DelayedUntil.After(now) {
			break
		}
		readyNode := heap.Pop(&q.delay).(*node[K, V])
		// Move from delay -> prio and exp heaps. Heap Push updates node indices.
		heap.Push(&q.prio, readyNode)
		if !readyNode.item.ExpiresAt.IsZero() {
			heap.Push(&q.exp, readyNode)
		}
	}
}

// removeExpiredNodesLocked removes expired items from the main heaps.
func (q *InMemory[K, V]) removeExpiredNodesLocked() {
	now := q.now()

	// Remove expired items from the main heaps
	for len(q.exp) > 0 {
		// Peek at the latest expiry date, if not expired, we're done with expired items
		if q.exp[0].item.ExpiresAt.IsZero() || q.exp[0].item.ExpiresAt.After(now) {
			break
		}

		// If we get here, the top node is expired; remove from the main heaps
		expiredNode := heap.Pop(&q.exp).(*node[K, V])
		heap.Remove(&q.prio, expiredNode.idxPrio)

		// Remove from items map
		delete(q.items, expiredNode.item.Key)
	}
}

// updateExistingNodeLocked updates an existing node with new values and handles heap movements.
// Must be called with mu held.
func (q *InMemory[K, V]) updateExistingNodeLocked(existingNode *node[K, V], item WorkItem[K, V]) {
	now := q.now()
	oldDelayUntil := existingNode.item.DelayedUntil
	oldPriority := existingNode.item.Priority
	oldExpiry := existingNode.item.ExpiresAt

	// Update existing node
	existingNode.item = item

	// Determine delay state based on current time
	shouldBeDelayed := !item.DelayedUntil.IsZero() && item.DelayedUntil.After(now)
	wasDelayed := !oldDelayUntil.IsZero() && oldDelayUntil.After(now)

	// Handle heap movements based on delay state changes
	switch {
	case shouldBeDelayed && !wasDelayed:
		// Moving from main heaps to delay heap
		q.moveNodeToDelayHeapLocked(existingNode)
	case !shouldBeDelayed && wasDelayed:
		// Moving from delay heap to main heaps
		q.moveNodeFromDelayToMainHeapsLocked(existingNode)
	case shouldBeDelayed && wasDelayed:
		// Staying in delay heap - fix position if delay time changed
		if delayChanged(oldDelayUntil, item.DelayedUntil) {
			heap.Fix(&q.delay, existingNode.idxDelay)
			q.rearmDelayTimerLocked()
		}
	case !shouldBeDelayed && !wasDelayed:
		// Staying in main heaps - fix positions if priority or expiry changed
		if priorityChanged(oldPriority, item.Priority) {
			heap.Fix(&q.prio, existingNode.idxPrio)
		}
		if expiryChanged(oldExpiry, item.ExpiresAt) {
			hadExpiry := !oldExpiry.IsZero()
			hasExpiry := !item.ExpiresAt.IsZero()

			switch {
			case hasExpiry && !hadExpiry:
				heap.Push(&q.exp, existingNode)
			case !hasExpiry && hadExpiry:
				heap.Remove(&q.exp, existingNode.idxExp)
			case hasExpiry && hadExpiry:
				heap.Fix(&q.exp, existingNode.idxExp)
			}
		}
	}
}

// pushNewNodeLocked pushes a new node to the appropriate heaps and adds it to the items map.
// Must be called with mu held.
func (q *InMemory[K, V]) pushNewNodeLocked(item WorkItem[K, V]) {
	key := item.Key
	now := q.now()

	// Create new node
	q.nextID++
	n := &node[K, V]{
		item:     item,
		id:       q.nextID,
		idxPrio:  -1,
		idxExp:   -1,
		idxDelay: -1,
	}

	shouldBeDelayed := !item.DelayedUntil.IsZero() && item.DelayedUntil.After(now)

	// Determine which heaps to push to
	if shouldBeDelayed {
		heap.Push(&q.delay, n)

		// entering delay heap may affect timer
		q.rearmDelayTimerLocked()
	} else {
		// No delay or delay is past, push to main heaps
		heap.Push(&q.prio, n)
		if !item.ExpiresAt.IsZero() {
			heap.Push(&q.exp, n)
		}
	}

	// Add to items map. this will be used to deduplicate items.
	q.items[key] = n
}
