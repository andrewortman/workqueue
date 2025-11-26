# workqueue

[![Go Reference](https://pkg.go.dev/badge/github.com/andrewortman/workqueue.svg)](https://pkg.go.dev/github.com/andrewortman/workqueue)
[![Go Report Card](https://goreportcard.com/badge/github.com/andrewortman/workqueue)](https://goreportcard.com/report/github.com/andrewortman/workqueue)
[![Release](https://img.shields.io/github/v/release/andrewortman/workqueue.svg?style=flat-square)](https://github.com/andrewortman/workqueue/releases/latest)

**workqueue** is a high-performance task queue for Go applications. It provides powerful features like task expiration, delayed execution, deduplication by key, and prioritization, all without external dependencies. The current implementation is in-memory, ensuring low latency and high throughput for any workload. It was designed as a [crawl frontier](https://en.wikipedia.org/wiki/Crawl_frontier).

## Features

- **Fast Processing**: All operations (put, update, take, insert, expiration, delay) are all done in either `O(n*log(n))` time or better using several priority queues to maintain state.
- **Task Expiration**: Set an expiration time for items to ensure they are processed within a specific timeframe. Expired items are automatically removed.
- **Delayed Execution**: Schedule items to become available for processing only after a certain delay.
- **Prioritization**: Assign priorities to items to control the order of execution. Higher priority items are processed first.
- **FIFO Tie-Breaking**: Items with the same priority are processed in a first-in, first-out (FIFO) order.
- **Goroutine-Safe**: Safe for concurrent producers and consumers.

## Usage

### Creating a Work Queue

To get started, create a new in-memory work queue. The queue is generic and can handle any comparable key and value types.

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/andrewortman/workqueue"
)

func main() {
	// Create a new work queue for string keys and values
	queue := workqueue.NewInMemory[string, string]()

	// The rest of the examples assume this `queue` instance
	// and a `context.Context` variable named `ctx` are available.
	ctx := context.Background()
	_ = queue
	_ = ctx
}
```

### Configuration Options

The queue can be configured with functional options:

```go
	// With a custom time provider (useful for testing)
	queue := workqueue.NewInMemory[string, string](
		workqueue.WithTimeProvider[string, string](myTimeProvider),
	)

	// With a capacity limit
	queue := workqueue.NewInMemory[string, string](
		workqueue.WithCapacity[string, string](1000),
	)
```

#### `WithTimeProvider`
Injects a custom `TimeProvider` for controlling time in tests or other scenarios.

#### `WithCapacity`
Sets a maximum number of items the queue can hold. When at capacity:
- `Put` returns `ErrAtCapacity` and performs no mutations
- `PutOrUpdate` returns `ErrAtCapacity` and performs no mutations if any new items would exceed capacity; a batch containing only updates to existing keys will succeed

### Public API

#### `Put`
Adds one or more new items to the queue. It returns an error if an item with the same key already exists.

```go
	item := workqueue.WorkItem[string, string]{
		Key:   "task1",
		Value: "process this data",
	}

	if err := queue.Put(ctx, item); err != nil {
		fmt.Println("Error adding item:", err)
	}
```

#### `Update`
Updates one or more existing items. It returns an error if an item's key is not found in the queue.

This can be used to update any field of the work item - eg priority, delay, or expiration.

```go
	updatedItem := workqueue.WorkItem[string, string]{
		Key:   "task1",
		Value: "updated data",
	}

	if err := queue.Update(ctx, updatedItem); err != nil {
		fmt.Println("Error updating item:", err)
	}
```

#### `PutOrUpdate`
Adds new items or updates existing ones. This is useful when you want to ensure an item is in the queue, regardless of whether it was there before. 

```go
	newItem := workqueue.WorkItem[string, string]{
		Key:   "task2",
		Value: "new or updated data",
	}
	if err := queue.PutOrUpdate(ctx, newItem); err != nil {
		fmt.Println("Error with PutOrUpdate:", err)
	}
```

#### `Remove`
Removes an item from the queue by its key. It returns an error if the key is not found.

```go
	if err := queue.Remove(ctx, "task1"); err != nil {
		fmt.Println("Error removing item:", err)
	}
```

#### `Take`
Blocks until an item is available, then returns it. `Take` is the primary method for consuming items from the queue. It respects context cancellation.

```go
	// This will block until an item is available
	takenItem, err := queue.Take(ctx)
	if err != nil {
		fmt.Println("Error taking item:", err)
		return
	}
	fmt.Println("Processing item:", takenItem.Key)
```

#### `TakeMany`
Blocks until `n` items are available, and returns them as a slice. Like `Take`, it respects context cancellation.

```go
	// This will block until 2 items are available
	items, err := queue.TakeMany(ctx, 2)
	if err != nil {
		fmt.Println("Error taking many items:", err)
		return
	}
	fmt.Println("Processing batch of", len(items), "items")
```

#### Context Handling
All blocking operations (`Take`, `TakeMany`) and synchronous operations respect context cancellation. If the context is canceled while an operation is in progress, the function will unblock and return a context error.

```go
	// Create a context that will be canceled after 1 second
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	// If no item is available within 1 second, Take will return a context error.
	_, err := queue.Take(ctx)
	if err != nil {
		// This will print "context deadline exceeded"
		fmt.Println(err)
	}
```

#### `Size`
Returns the number of pending and delayed items in the queue.

```go
	size, err := queue.Size(ctx)
	if err != nil {
		fmt.Println("Error getting size:", err)
		return
	}
	fmt.Printf("Queue size: %d pending, %d delayed\n", size.Pending, size.Delayed)
```

#### `UpdateConditional`
Updates existing items only when a predicate returns true. The predicate receives a copy of the existing item and the new item.

```go
	shouldUpdate := func(existing workqueue.WorkItem[string, string], new workqueue.WorkItem[string, string]) bool {
		// Only update if the new priority is higher or the value changed
		return new.Priority > existing.Priority || new.Value != existing.Value
	}

	err := queue.UpdateConditional(ctx, shouldUpdate,
		workqueue.WorkItem[string, string]{Key: "task1", Value: "maybe update", Priority: 50},
	)
	if err != nil {
		fmt.Println("Error conditional update:", err)
	}
```

#### `PutOrUpdateConditional`
Inserts new items or updates existing items only when a predicate returns true. For existing items, the predicate receives a pointer to a copy of the existing item; for new items, the pointer is `nil`.

```go
	shouldPutOrUpdate := func(existing *workqueue.WorkItem[string, string], new workqueue.WorkItem[string, string]) bool {
		if existing == nil {
			// Only insert if priority >= 10
			return new.Priority >= 10
		}
		// For existing items, only update if expiry or delay changed
		return !existing.ExpiresAt.Equal(new.ExpiresAt) || !existing.DelayedUntil.Equal(new.DelayedUntil)
	}

	err := queue.PutOrUpdateConditional(ctx, shouldPutOrUpdate,
		workqueue.WorkItem[string, string]{Key: "task2", Value: "maybe insert/update", Priority: 20},
	)
	if err != nil {
		fmt.Println("Error conditional putOrUpdate:", err)
	}
```

## Performance

### Complexity

| Method       | Time Complexity              | Space Complexity |
|--------------|------------------------------|------------------|
| `Put`        | O(log n)                     | O(1)             |
| `Update`     | O(log n)                     | O(1)             |
| `PutOrUpdate`| O(log n)                     | O(1)             |
| `Remove`     | O(log n)                     | O(1)             |
| `Take`       | O(log n)                     | O(1)             |
| `TakeMany`   | O(k \* log n) for k items    | O(k) for k items |
| `Size`       | O(log n)                     | O(1)             |

_n = number of total items in the queue_