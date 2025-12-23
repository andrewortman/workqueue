# Function Queue with Worker Pool Example

This example demonstrates how to use `workqueue` to manage and execute functions in a worker pool pattern.

## What This Example Demonstrates

1. **Storing functions as work items**: Shows how to store executable functions/closures in the queue by wrapping them in a struct (since functions themselves aren't comparable in Go).

2. **Worker pool pattern**: Creates a pool of 5 concurrent goroutines that pull work from the shared queue and execute tasks in parallel.

3. **Priority-based execution**: Tasks with higher priority values are executed first. The example includes various tasks with different priorities (40-100).

4. **Diverse task types**: Demonstrates different types of work functions:
   - Database operations (backup)
   - Communication (email notifications)
   - Data processing (reports, indexing)
   - Maintenance (cleanup, archiving)

5. **Graceful shutdown**: Workers detect when the queue is empty and shut down cleanly using context timeouts.

## How to Run

```bash
cd examples/function-queue
go run main.go
```

## Expected Output

The example will:
- Add 8 different tasks to the queue with varying priorities
- Start 5 workers that process tasks concurrently
- Show which worker executes which task
- Display task execution in priority order (highest first)
- Show workers shutting down gracefully when queue is empty

## Key Concepts

### Worker Pool Pattern
A worker pool is a common concurrency pattern where:
- Multiple goroutines (workers) run concurrently
- Workers pull work items from a shared queue
- Work is distributed automatically across available workers
- The pattern provides controlled concurrency and load balancing

### Storing Functions in the Queue

Since Go functions aren't comparable (can't be used as map keys or compared with `==`), we can't use them directly as the `Key` or `Value` type in workqueue. Instead, we:
1. Use an ID or name as the key (which is comparable)
2. Store the function in a struct as the value
3. Extract and execute the function when the task is taken from the queue

### workqueue Features Used

- **Priority**: Higher priority tasks (e.g., Database Backup: 100) execute before lower priority tasks (e.g., Archive Old Logs: 40)
- **Take**: Workers block until a task is available
- **Context with timeout**: Allows workers to detect when no more work is available
- **Concurrent access**: Multiple workers safely access the same queue

## Adapting for Production

In a production system, you might:
1. Add error handling and retry logic for failed tasks
2. Implement task timeouts to prevent hanging
3. Add monitoring and metrics (tasks processed, queue depth, etc.)
4. Store task results or errors for later review
5. Support task cancellation
6. Implement a dead-letter queue for repeatedly failing tasks
7. Add task dependencies (task B runs only after task A completes)
8. Persist tasks to disk/database for durability
9. Support scheduled/delayed task execution using `DelayedUntil`
10. Add task expiration using `ExpiresAt` for time-sensitive work

## Real-World Use Cases

This pattern is useful for:
- Background job processing
- Task scheduling systems
- Event-driven architectures
- Batch processing pipelines
- Microservices work distribution
- Async operation handling
- Load balancing across workers
