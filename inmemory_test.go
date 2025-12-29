package workqueue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemory_PriorityOrdering(t *testing.T) {
	q := NewInMemory[string, string]()
	ctx := context.Background()

	mockTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	q.now = func() time.Time { return mockTime }
	expiry := mockTime.Add(time.Hour)

	err := q.Put(ctx,
		WorkItem[string, string]{Key: "low", Value: "low-priority", Priority: 10, ExpiresAt: expiry},
		WorkItem[string, string]{Key: "high", Value: "high-priority", Priority: 100, ExpiresAt: expiry},
		WorkItem[string, string]{Key: "medium", Value: "medium-priority", Priority: 50, ExpiresAt: expiry},
	)
	require.NoError(t, err)

	item, err := q.Take(ctx)
	require.NoError(t, err)
	assert.Equal(t, "high-priority", item.Value)

	item, err = q.Take(ctx)
	require.NoError(t, err)
	assert.Equal(t, "medium-priority", item.Value)

	item, err = q.Take(ctx)
	require.NoError(t, err)
	assert.Equal(t, "low-priority", item.Value)
}

func TestInMemory_TakeMany(t *testing.T) {
	t.Run("basic functionality", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		items := []WorkItem[string, string]{
			{Key: "a", Value: "a", Priority: 100, ExpiresAt: expiry},
			{Key: "b", Value: "b", Priority: 90, ExpiresAt: expiry},
			{Key: "c", Value: "c", Priority: 80, ExpiresAt: expiry},
		}
		require.NoError(t, q.Put(ctx, items...))

		taken, err := q.TakeMany(ctx, 3)
		require.NoError(t, err)
		require.Len(t, taken, 3)
		assert.Equal(t, "a", taken[0].Value)
		assert.Equal(t, "b", taken[1].Value)
		assert.Equal(t, "c", taken[2].Value)
	})

	t.Run("context cancellation", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx, cancel := context.WithCancel(context.Background())
		expiry := time.Now().Add(time.Hour)

		require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a", ExpiresAt: expiry}))

		var wg sync.WaitGroup
		wg.Add(1)

		waitCh := make(chan struct{})
		q.mu.Lock()
		q.onTakeWait = func() {
			// Use a channel to signal that the consumer is waiting.
			close(waitCh)
		}
		q.mu.Unlock()

		var takeErr error
		var takenItems []WorkItem[string, string]
		go func() {
			defer wg.Done()
			takenItems, takeErr = q.TakeMany(ctx, 2)
		}()

		<-waitCh // Block until the consumer signals it's waiting.
		cancel() // Now, cancel the context.
		wg.Wait()

		require.Error(t, takeErr)
		assert.ErrorIs(t, takeErr, context.Canceled)
		require.Len(t, takenItems, 0)

		// Item should remain in the queue
		size, err := q.Size(context.Background())
		require.NoError(t, err)
		assert.Equal(t, 1, size.Pending)
	})
}

func TestInMemory_TakeUpTo(t *testing.T) {
	t.Run("takes all available when fewer than max", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		items := []WorkItem[string, string]{
			{Key: "a", Value: "a", Priority: 100, ExpiresAt: expiry},
			{Key: "b", Value: "b", Priority: 90, ExpiresAt: expiry},
		}
		require.NoError(t, q.Put(ctx, items...))

		taken, err := q.TakeUpTo(ctx, 5)
		require.NoError(t, err)
		require.Len(t, taken, 2)
		assert.Equal(t, "a", taken[0].Value)
		assert.Equal(t, "b", taken[1].Value)
	})

	t.Run("caps at max items when more available", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		items := []WorkItem[string, string]{
			{Key: "a", Value: "a", Priority: 100, ExpiresAt: expiry},
			{Key: "b", Value: "b", Priority: 90, ExpiresAt: expiry},
			{Key: "c", Value: "c", Priority: 80, ExpiresAt: expiry},
			{Key: "d", Value: "d", Priority: 70, ExpiresAt: expiry},
		}
		require.NoError(t, q.Put(ctx, items...))

		taken, err := q.TakeUpTo(ctx, 2)
		require.NoError(t, err)
		require.Len(t, taken, 2)
		assert.Equal(t, "a", taken[0].Value)
		assert.Equal(t, "b", taken[1].Value)

		// Verify remaining items
		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, size.Pending)
	})

	t.Run("blocks until at least one item available", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		var wg sync.WaitGroup
		wg.Add(1)

		waitCh := make(chan struct{})
		q.mu.Lock()
		q.onTakeWait = func() {
			close(waitCh)
		}
		q.mu.Unlock()

		var taken []WorkItem[string, string]
		var takeErr error
		go func() {
			defer wg.Done()
			taken, takeErr = q.TakeUpTo(ctx, 5)
		}()

		<-waitCh // Wait for consumer to be blocking

		// Add items while consumer is waiting
		require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a", Priority: 100, ExpiresAt: expiry}))
		require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "b", Value: "b", Priority: 90, ExpiresAt: expiry}))

		wg.Wait()

		require.NoError(t, takeErr)
		require.Len(t, taken, 2)
	})

	t.Run("context cancellation", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx, cancel := context.WithCancel(context.Background())

		var wg sync.WaitGroup
		wg.Add(1)

		waitCh := make(chan struct{})
		q.mu.Lock()
		q.onTakeWait = func() {
			close(waitCh)
		}
		q.mu.Unlock()

		var taken []WorkItem[string, string]
		var takeErr error
		go func() {
			defer wg.Done()
			taken, takeErr = q.TakeUpTo(ctx, 5)
		}()

		<-waitCh
		cancel()
		wg.Wait()

		require.Error(t, takeErr)
		assert.ErrorIs(t, takeErr, context.Canceled)
		require.Len(t, taken, 0)
	})

	t.Run("returns nil for zero or negative max", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a", ExpiresAt: expiry}))

		taken, err := q.TakeUpTo(ctx, 0)
		require.NoError(t, err)
		require.Nil(t, taken)

		taken, err = q.TakeUpTo(ctx, -1)
		require.NoError(t, err)
		require.Nil(t, taken)

		// Item should still be in queue
		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, size.Pending)
	})

	t.Run("respects priority ordering", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		// Add items in random priority order
		items := []WorkItem[string, string]{
			{Key: "low", Value: "low", Priority: 10, ExpiresAt: expiry},
			{Key: "high", Value: "high", Priority: 100, ExpiresAt: expiry},
			{Key: "medium", Value: "medium", Priority: 50, ExpiresAt: expiry},
		}
		require.NoError(t, q.Put(ctx, items...))

		taken, err := q.TakeUpTo(ctx, 2)
		require.NoError(t, err)
		require.Len(t, taken, 2)
		assert.Equal(t, "high", taken[0].Value)
		assert.Equal(t, "medium", taken[1].Value)
	})

	t.Run("fifo scheduling with TakeMany", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		var wg sync.WaitGroup
		wg.Add(2)

		firstConsumerWaitingCh := make(chan struct{})
		secondConsumerWaitingCh := make(chan struct{})

		// Start first consumer using TakeMany (wants exactly 2)
		var takenMany []WorkItem[string, string]
		go func() {
			defer wg.Done()
			var err error
			takenMany, err = q.TakeMany(ctx, 2)
			require.NoError(t, err)
		}()

		q.mu.Lock()
		q.onTakeWait = func() {
			close(firstConsumerWaitingCh)
		}
		q.mu.Unlock()
		<-firstConsumerWaitingCh

		// Start second consumer using TakeUpTo
		var takenUpTo []WorkItem[string, string]
		go func() {
			defer wg.Done()
			var err error
			takenUpTo, err = q.TakeUpTo(ctx, 10)
			require.NoError(t, err)
		}()

		q.mu.Lock()
		q.onTakeWait = func() {
			close(secondConsumerWaitingCh)
		}
		q.mu.Unlock()
		<-secondConsumerWaitingCh
		q.mu.Lock()
		q.onTakeWait = nil
		q.mu.Unlock()

		// Add items
		require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a", ExpiresAt: expiry, Priority: 1}))
		require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "b", Value: "b", ExpiresAt: expiry, Priority: 2}))
		require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "c", Value: "c", ExpiresAt: expiry, Priority: 3}))

		wg.Wait()

		// First consumer (TakeMany) should get first 2 items by priority
		require.Len(t, takenMany, 2)
		assert.Equal(t, "c", takenMany[0].Value)
		assert.Equal(t, "b", takenMany[1].Value)

		// Second consumer (TakeUpTo) should get remaining item
		require.Len(t, takenUpTo, 1)
		assert.Equal(t, "a", takenUpTo[0].Value)
	})
}

func TestInMemory_TakeFIFOScheduling(t *testing.T) {
	q := NewInMemory[string, string]()
	ctx := context.Background()
	expiry := time.Now().Add(time.Hour)

	var wg sync.WaitGroup
	wg.Add(2)

	// Use channels to synchronize with the consumers.
	firstConsumerWaitingCh := make(chan struct{})
	secondConsumerWaitingCh := make(chan struct{})

	// Start a consumer that wants 2 items.
	var takenMany []WorkItem[string, string]
	go func() {
		defer wg.Done()
		var err error
		takenMany, err = q.TakeMany(ctx, 2)
		require.NoError(t, err)
	}()

	// The first time the hook is called, it's the first consumer.
	q.mu.Lock()
	q.onTakeWait = func() {
		close(firstConsumerWaitingCh)
	}
	q.mu.Unlock()
	<-firstConsumerWaitingCh

	// Start a second consumer that wants 1 item.
	var takenOne WorkItem[string, string]
	go func() {
		defer wg.Done()
		var err error
		takenOne, err = q.Take(ctx)
		require.NoError(t, err)
	}()

	// The second time, it's the second consumer.
	q.mu.Lock()
	q.onTakeWait = func() {
		close(secondConsumerWaitingCh)
	}
	q.mu.Unlock()
	<-secondConsumerWaitingCh
	q.mu.Lock()
	q.onTakeWait = nil // Disable the hook.
	q.mu.Unlock()

	// At this point, we are certain that the first consumer is waiting for 2 items,
	// and the second consumer is waiting for 1 item, and they are in that order.

	// Add items, which will wake up the consumers.
	require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a", ExpiresAt: expiry, Priority: 1}))
	require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "b", Value: "b", ExpiresAt: expiry, Priority: 2}))
	require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "c", Value: "c", ExpiresAt: expiry, Priority: 3}))

	wg.Wait()

	// Verify that the first consumer got the first two items based on priority.
	require.Len(t, takenMany, 2)
	assert.Equal(t, "c", takenMany[0].Value) // Highest priority
	assert.Equal(t, "b", takenMany[1].Value)

	// Verify that the second consumer got the third item.
	assert.Equal(t, "a", takenOne.Value)
}

func TestInMemory_TTLExpiry(t *testing.T) {
	q := NewInMemory[string, string]()
	ctx := context.Background()

	mockTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	q.now = func() time.Time { return mockTime }

	err := q.Put(ctx,
		WorkItem[string, string]{Key: "expire-soon", Value: "expires-in-1s", ExpiresAt: mockTime.Add(time.Second)},
		WorkItem[string, string]{Key: "expire-later", Value: "expires-in-1h", ExpiresAt: mockTime.Add(time.Hour)},
	)
	require.NoError(t, err)

	mockTime = mockTime.Add(2 * time.Second)

	item, err := q.Take(ctx)
	require.NoError(t, err)
	assert.Equal(t, "expires-in-1h", item.Value)

	sizeResult, _ := q.Size(ctx)
	assert.Equal(t, 0, sizeResult.Pending+sizeResult.Delayed)
}

func TestInMemory_ExpirationScenarios(t *testing.T) {
	baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	testCases := []struct {
		name          string
		items         []WorkItem[string, string]
		timeAdvances  []time.Duration
		expectedTakes []string // values of items we expect to take
		expectedSize  SizeResult
		expectTakeErr error
	}{
		{
			name: "simple expiry",
			items: []WorkItem[string, string]{
				{Key: "a", Value: "a", ExpiresAt: baseTime.Add(5 * time.Second)},
				{Key: "b", Value: "b", ExpiresAt: baseTime.Add(15 * time.Second)},
			},
			timeAdvances:  []time.Duration{10 * time.Second}, // time is now baseTime + 10s
			expectedTakes: []string{"b"},
			expectedSize:  SizeResult{Pending: 0, Delayed: 0}, // after taking "b"
		},
		{
			name: "no expiry",
			items: []WorkItem[string, string]{
				{Key: "a", Value: "a"},
			},
			timeAdvances:  []time.Duration{100 * time.Hour},
			expectedTakes: []string{"a"},
			expectedSize:  SizeResult{Pending: 0, Delayed: 0},
		},
		{
			name: "all items expire",
			items: []WorkItem[string, string]{
				{Key: "a", Value: "a", ExpiresAt: baseTime.Add(1 * time.Second)},
				{Key: "b", Value: "b", ExpiresAt: baseTime.Add(2 * time.Second)},
			},
			timeAdvances:  []time.Duration{5 * time.Second},
			expectedTakes: []string{},
			expectedSize:  SizeResult{Pending: 0, Delayed: 0},
			expectTakeErr: context.DeadlineExceeded,
		},
		{
			name: "delayed item expires after becoming available",
			items: []WorkItem[string, string]{
				{Key: "a", Value: "a", DelayedUntil: baseTime.Add(5 * time.Second), ExpiresAt: baseTime.Add(10 * time.Second)},
			},
			timeAdvances: []time.Duration{
				6 * time.Second, // item becomes available
				5 * time.Second, // item expires (total 11s)
			},
			expectedTakes: []string{},
			expectedSize:  SizeResult{Pending: 0, Delayed: 0},
			expectTakeErr: context.DeadlineExceeded,
		},
		{
			name: "delayed item is taken before expiry",
			items: []WorkItem[string, string]{
				{Key: "a", Value: "a", DelayedUntil: baseTime.Add(5 * time.Second), ExpiresAt: baseTime.Add(10 * time.Second)},
			},
			timeAdvances:  []time.Duration{6 * time.Second}, // item becomes available
			expectedTakes: []string{"a"},
			expectedSize:  SizeResult{Pending: 0, Delayed: 0},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			q := NewInMemory[string, string]()
			ctx := context.Background()

			mockTime := baseTime
			q.now = func() time.Time { return mockTime }

			err := q.Put(ctx, tc.items...)
			require.NoError(t, err)

			for _, adv := range tc.timeAdvances {
				mockTime = mockTime.Add(adv)
			}

			// Try to take the expected items
			for _, expectedVal := range tc.expectedTakes {
				item, err := q.Take(ctx)
				require.NoError(t, err)
				assert.Equal(t, expectedVal, item.Value)
			}

			// Check for expected error on take if no items should be available
			if tc.expectTakeErr != nil {
				takeCtx, cancel := context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
				_, err := q.Take(takeCtx)
				require.Error(t, err)
				assert.ErrorIs(t, err, tc.expectTakeErr)
			}

			// Check final size
			size, err := q.Size(ctx)
			require.NoError(t, err)
			assert.Equal(t, tc.expectedSize, size)
		})
	}
}

func TestInMemory_PutOrUpdate(t *testing.T) {
	q := NewInMemory[string, string]()
	ctx := context.Background()
	expiry := time.Now().Add(time.Hour)

	err := q.PutOrUpdate(ctx, WorkItem[string, string]{Key: "dup", Value: "original", Priority: 50, ExpiresAt: expiry})
	require.NoError(t, err)

	err = q.PutOrUpdate(ctx, WorkItem[string, string]{Key: "dup", Value: "higher", Priority: 100, ExpiresAt: expiry})
	require.NoError(t, err)

	sizeResult, _ := q.Size(ctx)
	assert.Equal(t, 1, sizeResult.Pending+sizeResult.Delayed)

	item, err := q.Take(ctx)
	require.NoError(t, err)
	assert.Equal(t, "higher", item.Value)
}

func TestInMemory_ErrorTypes(t *testing.T) {
	t.Run("item already exists", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		err := q.Put(ctx, WorkItem[string, string]{Key: "exists", Value: "v", ExpiresAt: expiry})
		require.NoError(t, err)
		err = q.Put(ctx, WorkItem[string, string]{Key: "exists", Value: "v2", ExpiresAt: expiry})
		assert.ErrorIs(t, err, ErrItemExists)
	})

	t.Run("item not found", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()

		err := q.Update(ctx, WorkItem[string, string]{Key: "not-found", Value: "v"})
		assert.ErrorIs(t, err, ErrItemNotFound)

		err = q.Remove(ctx, "not-found")
		assert.ErrorIs(t, err, ErrItemNotFound)
	})

	t.Run("expiry before delay", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()

		delay := time.Now().Add(time.Minute)
		expiryBeforeDelay := delay.Add(-time.Second)
		err := q.Put(ctx, WorkItem[string, string]{Key: "invalid-delay", Value: "v", ExpiresAt: expiryBeforeDelay, DelayedUntil: delay})
		assert.ErrorIs(t, err, ErrExpiryBeforeDelay)
	})
}

func TestInMemory_DelayBasic(t *testing.T) {
	q := NewInMemory[string, string]()
	ctx := context.Background()

	mockTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	q.now = func() time.Time { return mockTime }

	expiry := mockTime.Add(time.Hour)
	delayUntil := mockTime.Add(10 * time.Second)

	err := q.Put(ctx, WorkItem[string, string]{Key: "delayed", Value: "delayed-item", Priority: 100, ExpiresAt: expiry, DelayedUntil: delayUntil})
	require.NoError(t, err)

	sizeResult, _ := q.Size(ctx)
	assert.Equal(t, 0, sizeResult.Pending)
	assert.Equal(t, 1, sizeResult.Delayed)

	popCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	_, err = q.Take(popCtx)
	assert.ErrorIs(t, err, context.DeadlineExceeded)

	mockTime = mockTime.Add(15 * time.Second)

	item, err := q.Take(ctx)
	require.NoError(t, err)
	assert.Equal(t, "delayed-item", item.Value)
}

func TestInMemory_Remove(t *testing.T) {
	t.Run("remove existing item", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		require.NoError(t, q.Put(ctx, WorkItem[string, string]{Key: "test", Value: "test-value", ExpiresAt: expiry}))
		size, _ := q.Size(ctx)
		require.Equal(t, 1, size.Pending)

		err := q.Remove(ctx, "test")
		require.NoError(t, err)

		size, _ = q.Size(ctx)
		assert.Equal(t, 0, size.Pending)
	})

	t.Run("remove non-existent item", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		err := q.Remove(ctx, "nonexistent")
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrItemNotFound)
	})
}

func TestInMemory_UpdateConditional(t *testing.T) {
	t.Run("updates when predicate returns true", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		err := q.Put(ctx, WorkItem[string, string]{Key: "test", Value: "old", Priority: 50, ExpiresAt: expiry})
		require.NoError(t, err)

		shouldUpdate := func(existing WorkItem[string, string], new WorkItem[string, string]) bool {
			return new.Priority > existing.Priority
		}

		err = q.UpdateConditional(ctx, shouldUpdate, WorkItem[string, string]{Key: "test", Value: "new", Priority: 100, ExpiresAt: expiry})
		require.NoError(t, err)

		item, err := q.Take(ctx)
		require.NoError(t, err)
		assert.Equal(t, "new", item.Value)
		assert.Equal(t, int64(100), item.Priority)
	})

	t.Run("skips update when predicate returns false", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		err := q.Put(ctx, WorkItem[string, string]{Key: "test", Value: "old", Priority: 100, ExpiresAt: expiry})
		require.NoError(t, err)

		shouldUpdate := func(existing WorkItem[string, string], new WorkItem[string, string]) bool {
			return new.Priority > existing.Priority
		}

		err = q.UpdateConditional(ctx, shouldUpdate, WorkItem[string, string]{Key: "test", Value: "new", Priority: 50, ExpiresAt: expiry})
		require.NoError(t, err)

		item, err := q.Take(ctx)
		require.NoError(t, err)
		assert.Equal(t, "old", item.Value)
		assert.Equal(t, int64(100), item.Priority)
	})

	t.Run("returns error for non-existent item", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		shouldUpdate := func(existing WorkItem[string, string], new WorkItem[string, string]) bool {
			return true
		}

		err := q.UpdateConditional(ctx, shouldUpdate, WorkItem[string, string]{Key: "nonexistent", Value: "value", ExpiresAt: expiry})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrItemNotFound)
	})
}

func TestInMemory_WithTimeProvider(t *testing.T) {
	mockTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	tp := &mockTimeProvider{now: mockTime}

	q := NewInMemory[string, string](WithTimeProvider[string, string](tp))
	ctx := context.Background()

	// Add item that expires in 5 seconds
	err := q.Put(ctx, WorkItem[string, string]{Key: "test", Value: "test", ExpiresAt: mockTime.Add(5 * time.Second)})
	require.NoError(t, err)

	// Advance time past expiry
	tp.now = mockTime.Add(10 * time.Second)

	// Item should be expired
	size, err := q.Size(ctx)
	require.NoError(t, err)
	assert.Equal(t, 0, size.Pending)
}

type mockTimeProvider struct {
	now time.Time
}

func (m *mockTimeProvider) Now() time.Time {
	return m.now
}

func TestInMemory_WithCapacity(t *testing.T) {
	t.Run("put fails at capacity", func(t *testing.T) {
		q := NewInMemory[string, string](WithCapacity[string, string](2))
		ctx := context.Background()

		err := q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a"})
		require.NoError(t, err)

		err = q.Put(ctx, WorkItem[string, string]{Key: "b", Value: "b"})
		require.NoError(t, err)

		err = q.Put(ctx, WorkItem[string, string]{Key: "c", Value: "c"})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAtCapacity)

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 2, size.Pending)
	})

	t.Run("put batch fails if it would exceed capacity", func(t *testing.T) {
		q := NewInMemory[string, string](WithCapacity[string, string](2))
		ctx := context.Background()

		err := q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a"})
		require.NoError(t, err)

		// Trying to add 2 items when only 1 slot is available should fail
		err = q.Put(ctx,
			WorkItem[string, string]{Key: "b", Value: "b"},
			WorkItem[string, string]{Key: "c", Value: "c"},
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAtCapacity)

		// Original item should still be there
		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, size.Pending)
	})

	t.Run("put or update allows updates at capacity", func(t *testing.T) {
		q := NewInMemory[string, string](WithCapacity[string, string](2))
		ctx := context.Background()

		err := q.Put(ctx,
			WorkItem[string, string]{Key: "a", Value: "a"},
			WorkItem[string, string]{Key: "b", Value: "b"},
		)
		require.NoError(t, err)

		// Update existing item should work
		err = q.PutOrUpdate(ctx, WorkItem[string, string]{Key: "a", Value: "updated"})
		require.NoError(t, err)

		// Verify the update
		item, err := q.Take(ctx)
		require.NoError(t, err)
		assert.Equal(t, "updated", item.Value)
	})

	t.Run("put or update fails for new items at capacity", func(t *testing.T) {
		q := NewInMemory[string, string](WithCapacity[string, string](2))
		ctx := context.Background()

		err := q.Put(ctx,
			WorkItem[string, string]{Key: "a", Value: "a"},
			WorkItem[string, string]{Key: "b", Value: "b"},
		)
		require.NoError(t, err)

		// New item should fail
		err = q.PutOrUpdate(ctx, WorkItem[string, string]{Key: "c", Value: "c"})
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAtCapacity)
	})

	t.Run("put or update mixed batch fails if new items exceed capacity", func(t *testing.T) {
		q := NewInMemory[string, string](WithCapacity[string, string](2))
		ctx := context.Background()

		err := q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a"})
		require.NoError(t, err)

		// Mix of update and new items - should fail since new item would exceed capacity
		err = q.PutOrUpdate(ctx,
			WorkItem[string, string]{Key: "a", Value: "updated"},
			WorkItem[string, string]{Key: "b", Value: "b"},
			WorkItem[string, string]{Key: "c", Value: "c"}, // this would exceed capacity
		)
		require.Error(t, err)
		assert.ErrorIs(t, err, ErrAtCapacity)

		// Original item should be unchanged since batch failed
		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, size.Pending)
	})

	t.Run("zero capacity means unlimited", func(t *testing.T) {
		q := NewInMemory[string, string](WithCapacity[string, string](0))
		ctx := context.Background()

		// Should be able to add many items
		for i := 0; i < 100; i++ {
			err := q.Put(ctx, WorkItem[string, string]{Key: string(rune('a' + i)), Value: "v"})
			require.NoError(t, err)
		}

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 100, size.Pending)
	})

	t.Run("negative capacity means unlimited", func(t *testing.T) {
		q := NewInMemory[string, string](WithCapacity[string, string](-1))
		ctx := context.Background()

		err := q.Put(ctx, WorkItem[string, string]{Key: "a", Value: "a"})
		require.NoError(t, err)

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 1, size.Pending)
	})
}

func TestInMemory_PutOrUpdateConditional(t *testing.T) {
	t.Run("inserts new item when predicate returns true", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		shouldUpdate := func(existing *WorkItem[string, string], new WorkItem[string, string]) bool {
			return existing == nil || new.Priority > existing.Priority
		}

		err := q.PutOrUpdateConditional(ctx, shouldUpdate, WorkItem[string, string]{Key: "test", Value: "new", Priority: 100, ExpiresAt: expiry})
		require.NoError(t, err)

		item, err := q.Take(ctx)
		require.NoError(t, err)
		assert.Equal(t, "new", item.Value)
	})

	t.Run("skips insert when predicate returns false", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		shouldUpdate := func(existing *WorkItem[string, string], new WorkItem[string, string]) bool {
			return false
		}

		err := q.PutOrUpdateConditional(ctx, shouldUpdate, WorkItem[string, string]{Key: "test", Value: "new", Priority: 100, ExpiresAt: expiry})
		require.NoError(t, err)

		size, err := q.Size(ctx)
		require.NoError(t, err)
		assert.Equal(t, 0, size.Pending)
	})

	t.Run("updates existing item when predicate returns true", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		err := q.Put(ctx, WorkItem[string, string]{Key: "test", Value: "old", Priority: 50, ExpiresAt: expiry})
		require.NoError(t, err)

		shouldUpdate := func(existing *WorkItem[string, string], new WorkItem[string, string]) bool {
			return existing == nil || new.Priority > existing.Priority
		}

		err = q.PutOrUpdateConditional(ctx, shouldUpdate, WorkItem[string, string]{Key: "test", Value: "new", Priority: 100, ExpiresAt: expiry})
		require.NoError(t, err)

		item, err := q.Take(ctx)
		require.NoError(t, err)
		assert.Equal(t, "new", item.Value)
		assert.Equal(t, int64(100), item.Priority)
	})

	t.Run("skips update of existing item when predicate returns false", func(t *testing.T) {
		q := NewInMemory[string, string]()
		ctx := context.Background()
		expiry := time.Now().Add(time.Hour)

		err := q.Put(ctx, WorkItem[string, string]{Key: "test", Value: "old", Priority: 100, ExpiresAt: expiry})
		require.NoError(t, err)

		shouldUpdate := func(existing *WorkItem[string, string], new WorkItem[string, string]) bool {
			return existing == nil || new.Priority > existing.Priority
		}

		err = q.PutOrUpdateConditional(ctx, shouldUpdate, WorkItem[string, string]{Key: "test", Value: "new", Priority: 50, ExpiresAt: expiry})
		require.NoError(t, err)

		item, err := q.Take(ctx)
		require.NoError(t, err)
		assert.Equal(t, "old", item.Value)
		assert.Equal(t, int64(100), item.Priority)
	})
}
