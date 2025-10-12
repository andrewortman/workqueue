package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/andrewortman/workqueue"
)

// URLInfo stores information about a URL to be crawled
type URLInfo struct {
	URL         string
	LastFetched time.Time
}

func main() {
	// Create a new work queue for URL crawling
	// Key: URL string, Value: URLInfo
	queue := workqueue.NewInMemory[string, URLInfo](nil)
	ctx := context.Background()

	fmt.Println("=== URL Crawl Frontier Example ===")
	fmt.Println()

	// Seed URLs to crawl
	seedURLs := []string{
		"https://example.com",
		"https://example.com/page1",
		"https://example.com/page2",
		"https://example.com/page3",
		"https://example.com/blog",
	}

	now := time.Now()

	// Add seed URLs to the queue
	// URLs that haven't been fetched get the highest priority (using a large value)
	// They expire after 24 hours
	fmt.Println("Adding seed URLs to the queue...")
	for _, url := range seedURLs {
		item := workqueue.WorkItem[string, URLInfo]{
			Key: url,
			Value: URLInfo{
				URL:         url,
				LastFetched: time.Time{}, // Never fetched
			},
			Priority:  1000, // High priority for never-fetched URLs
			ExpiresAt: now.Add(24 * time.Hour),
		}
		if err := queue.Put(ctx, item); err != nil {
			log.Fatalf("Failed to add URL %s: %v", url, err)
		}
		fmt.Printf("  Added: %s (priority: %d, expires in 24h)\n", url, item.Priority)
	}

	fmt.Println()
	fmt.Println("Starting crawler workers...")
	fmt.Println()

	// Start 3 worker goroutines that fetch URLs
	const numWorkers = 3
	done := make(chan bool)

	for i := 1; i <= numWorkers; i++ {
		workerID := i
		go func() {
			fetchCount := 0
			// Each worker will process 2 URLs
			for fetchCount < 2 {
				// Take a URL from the queue
				item, err := queue.Take(ctx)
				if err != nil {
					log.Printf("Worker %d: error taking item: %v", workerID, err)
					break
				}

				urlInfo := item.Value
				fetchCount++

				// Simulate fetching the URL
				fmt.Printf("Worker %d: Fetching %s (fetch #%d)\n", workerID, urlInfo.URL, fetchCount)
				
				// Simulate varying fetch times (100-500ms)
				sleepTime := time.Duration(100+rand.Intn(400)) * time.Millisecond
				time.Sleep(sleepTime)

				fmt.Printf("Worker %d: Completed %s (took %v)\n", workerID, urlInfo.URL, sleepTime)

				// Re-add the URL with a 1-hour delay and lower priority
				// After being fetched, URLs get lower priority (older fetches = higher priority)
				// So we decrease the priority slightly with each fetch
				lastFetchTime := time.Now()
				updatedItem := workqueue.WorkItem[string, URLInfo]{
					Key: item.Key,
					Value: URLInfo{
						URL:         urlInfo.URL,
						LastFetched: lastFetchTime,
					},
					// Lower priority after being fetched (we use age-based priority)
					// The more recently fetched, the lower the priority
					Priority: 500, // Lower than never-fetched URLs
					// Delay for 1 hour before it can be fetched again
					DelayedUntil: lastFetchTime.Add(1 * time.Hour),
					// Still expires 24 hours from original add (in this demo, we keep original expiry)
					ExpiresAt: item.ExpiresAt,
				}

				// Use PutOrUpdate to re-add the URL (it was removed by Take)
				if err := queue.Put(ctx, updatedItem); err != nil {
					log.Printf("Worker %d: Failed to re-queue %s: %v", workerID, urlInfo.URL, err)
				} else {
					fmt.Printf("Worker %d: Re-queued %s (delayed 1h, priority: %d)\n",
						workerID, urlInfo.URL, updatedItem.Priority)
				}
				fmt.Println()
			}
			done <- true
		}()
	}

	// Wait for all workers to complete
	for i := 0; i < numWorkers; i++ {
		<-done
	}

	fmt.Println("=== Checking Queue Status ===")
	
	// Show the queue size
	size, err := queue.Size(ctx)
	if err != nil {
		log.Fatalf("Failed to get queue size: %v", err)
	}
	fmt.Printf("Queue size: %d pending, %d delayed\n", size.Pending, size.Delayed)
	fmt.Println()

	// Demonstrate that delayed items cannot be fetched immediately
	fmt.Println("Attempting to fetch another URL (should show delayed items)...")
	
	// Create a context with a short timeout
	timeoutCtx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	
	_, err = queue.Take(timeoutCtx)
	if err != nil {
		fmt.Printf("Expected timeout: %v\n", err)
		fmt.Println("(This is correct - all URLs are delayed for 1 hour)")
	}

	fmt.Println()
	fmt.Println("=== Summary ===")
	fmt.Println("This example demonstrated:")
	fmt.Println("  1. Adding URLs with priority (never-fetched = highest)")
	fmt.Println("  2. Workers fetching URLs from the queue")
	fmt.Println("  3. Re-queueing URLs with 1-hour delay after fetch")
	fmt.Println("  4. 24-hour expiration for URLs")
	fmt.Println("  5. Priority-based processing (higher priority first)")
	fmt.Println()
	fmt.Println("In a real crawler:")
	fmt.Println("  - URLs would actually be fetched via HTTP")
	fmt.Println("  - New URLs discovered would be added to the queue")
	fmt.Println("  - Priority could be based on various factors (age, importance, etc.)")
	fmt.Println("  - The crawler would run continuously, not just process a fixed number")
}
