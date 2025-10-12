package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/andrewortman/workqueue"
)

// TaskMetadata represents metadata about a task
// This is comparable and can be used in the workqueue
type TaskMetadata struct {
	ID       int
	Name     string
	Priority int64
}

// Task represents a work function to be executed
type Task struct {
	Metadata TaskMetadata
	Execute  func() error
}

func main() {
	// Create a work queue where:
	// - Key: task ID (int)
	// - Value: TaskMetadata (comparable)
	// We'll use a map to look up the actual functions
	queue := workqueue.NewInMemory[int, TaskMetadata](nil)
	ctx := context.Background()

	fmt.Println("=== Function Queue with Worker Pool Example ===")
	fmt.Println()

	// Create a map to store task functions
	// The queue will store TaskMetadata, and we'll look up functions here
	taskFunctions := make(map[int]func() error)

	// Create sample tasks with different behaviors
	tasks := []Task{
		{
			Metadata: TaskMetadata{ID: 1, Name: "Database Backup", Priority: 100},
			Execute: func() error {
				fmt.Println("    → Starting database backup...")
				time.Sleep(500 * time.Millisecond)
				fmt.Println("    → Database backup completed!")
				return nil
			},
		},
		{
			Metadata: TaskMetadata{ID: 2, Name: "Send Email Notification", Priority: 80},
			Execute: func() error {
				fmt.Println("    → Sending email notification...")
				time.Sleep(200 * time.Millisecond)
				fmt.Println("    → Email sent successfully!")
				return nil
			},
		},
		{
			Metadata: TaskMetadata{ID: 3, Name: "Generate Report", Priority: 90},
			Execute: func() error {
				fmt.Println("    → Generating monthly report...")
				time.Sleep(700 * time.Millisecond)
				fmt.Println("    → Report generated!")
				return nil
			},
		},
		{
			Metadata: TaskMetadata{ID: 4, Name: "Clean Temporary Files", Priority: 50},
			Execute: func() error {
				fmt.Println("    → Cleaning temporary files...")
				time.Sleep(300 * time.Millisecond)
				fmt.Println("    → Cleanup completed!")
				return nil
			},
		},
		{
			Metadata: TaskMetadata{ID: 5, Name: "Update Cache", Priority: 85},
			Execute: func() error {
				fmt.Println("    → Updating cache...")
				time.Sleep(400 * time.Millisecond)
				fmt.Println("    → Cache updated!")
				return nil
			},
		},
		{
			Metadata: TaskMetadata{ID: 6, Name: "Process API Webhooks", Priority: 95},
			Execute: func() error {
				fmt.Println("    → Processing webhooks...")
				time.Sleep(250 * time.Millisecond)
				fmt.Println("    → Webhooks processed!")
				return nil
			},
		},
		{
			Metadata: TaskMetadata{ID: 7, Name: "Archive Old Logs", Priority: 40},
			Execute: func() error {
				fmt.Println("    → Archiving old logs...")
				time.Sleep(600 * time.Millisecond)
				fmt.Println("    → Logs archived!")
				return nil
			},
		},
		{
			Metadata: TaskMetadata{ID: 8, Name: "Index Search Data", Priority: 75},
			Execute: func() error {
				fmt.Println("    → Indexing search data...")
				time.Sleep(450 * time.Millisecond)
				fmt.Println("    → Indexing complete!")
				return nil
			},
		},
	}

	// Add tasks to the queue and store functions in the map
	fmt.Println("Adding tasks to the queue...")
	for _, task := range tasks {
		// Store the function in our map
		taskFunctions[task.Metadata.ID] = task.Execute
		
		// Add metadata to the queue
		item := workqueue.WorkItem[int, TaskMetadata]{
			Key:      task.Metadata.ID,
			Value:    task.Metadata,
			Priority: task.Metadata.Priority,
		}
		if err := queue.Put(ctx, item); err != nil {
			log.Fatalf("Failed to add task %d: %v", task.Metadata.ID, err)
		}
		fmt.Printf("  Added task %d: %s (priority: %d)\n", task.Metadata.ID, task.Metadata.Name, task.Metadata.Priority)
	}

	fmt.Println()
	fmt.Println("Starting worker pool (5 workers)...")
	fmt.Println()

	// Create a worker pool
	const numWorkers = 5
	var wg sync.WaitGroup
	
	// Track completed tasks
	completed := make(chan int, len(tasks))

	// Start workers
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		workerID := i
		go func() {
			defer wg.Done()
			
			for {
				// Try to take a task from the queue with a timeout
				// This allows workers to exit gracefully when queue is empty
				timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
				item, err := queue.Take(timeoutCtx)
				cancel()

				if err != nil {
					// Context timeout means no more tasks available
					if err == context.DeadlineExceeded {
						fmt.Printf("Worker %d: No more tasks, shutting down\n", workerID)
						return
					}
					log.Printf("Worker %d: Error taking task: %v", workerID, err)
					return
				}

				metadata := item.Value
				
				// Look up the function for this task
				taskFunc, ok := taskFunctions[metadata.ID]
				if !ok {
					log.Printf("Worker %d: Function not found for task %d", workerID, metadata.ID)
					continue
				}
				
				// Execute the task
				fmt.Printf("Worker %d: Executing task %d - %s\n", workerID, metadata.ID, metadata.Name)
				
				// Run the function
				if err := taskFunc(); err != nil {
					log.Printf("Worker %d: Task %d failed: %v", workerID, metadata.ID, err)
				} else {
					fmt.Printf("Worker %d: Task %d completed successfully\n", workerID, metadata.ID)
				}
				
				completed <- metadata.ID
				fmt.Println()
			}
		}()
	}

	// Wait for all workers to finish
	wg.Wait()

	// Close the completed channel and collect results
	close(completed)
	completedTasks := []int{}
	for taskID := range completed {
		completedTasks = append(completedTasks, taskID)
	}

	fmt.Println("=== Summary ===")
	fmt.Printf("Total tasks completed: %d\n", len(completedTasks))
	fmt.Println()
	fmt.Println("This example demonstrated:")
	fmt.Println("  1. Storing functions/closures as work items")
	fmt.Println("  2. Priority-based execution (higher priority tasks run first)")
	fmt.Println("  3. Worker pool pattern with 5 concurrent goroutines")
	fmt.Println("  4. Each worker pulling and executing tasks from the queue")
	fmt.Println("  5. Graceful shutdown when queue is empty")
	fmt.Println()
	fmt.Println("Note: Due to concurrent execution, output order may vary")
	fmt.Println("      but high-priority tasks generally execute earlier.")
}
