# URL Crawl Frontier Example

This example demonstrates how to use `workqueue` as a URL crawl frontier - a queue that manages URLs to be fetched by a web crawler.

## What This Example Demonstrates

1. **Priority-based URL fetching**: URLs that have never been fetched get the highest priority (1000), while URLs that have been fetched get lower priority (500). In a real crawler, you could implement age-based priority where older fetches get higher priority.

2. **Delayed re-fetching**: After a URL is fetched, it's re-added to the queue with a 1-hour delay using `DelayedUntil`. This prevents the same URL from being fetched too frequently.

3. **24-hour expiration**: All URLs expire and are automatically removed from the queue after 24 hours using `ExpiresAt`.

4. **Multiple workers**: The example uses 3 concurrent worker goroutines that fetch URLs from the queue, simulating a realistic crawler architecture.

5. **Queue operations**: Demonstrates `Put` (adding new URLs), `Take` (fetching URLs to process), and checking queue status with `Size`.

## How to Run

```bash
cd examples/url-crawler
go run main.go
```

## Expected Output

The example will:
- Add 5 seed URLs to the queue
- Start 3 workers that process URLs concurrently
- Show each worker fetching URLs and re-queueing them with a delay
- Display the final queue state showing delayed items
- Demonstrate that delayed URLs cannot be fetched immediately

## Key Concepts

### Crawl Frontier
A crawl frontier is a data structure that manages which URLs a web crawler should fetch next. It typically handles:
- Politeness policies (delay between fetches from the same host)
- Prioritization (important pages first)
- Duplicate detection (don't fetch the same URL twice)
- URL expiration (remove stale URLs)

### workqueue Features Used

- **Priority**: Higher priority URLs are fetched first
- **DelayedUntil**: Prevents immediate re-fetching of URLs (politeness policy)
- **ExpiresAt**: Automatically removes old URLs from the queue
- **Take**: Workers block until a URL is available for fetching
- **Put**: Add new URLs or re-add previously fetched URLs

## Adapting for Production

In a production crawler, you would:
1. Actually fetch URLs using an HTTP client
2. Parse fetched pages to discover new URLs
3. Implement host-based delays (not just global delays)
4. Add duplicate detection logic
5. Persist the queue to disk/database
6. Handle robots.txt and other politeness policies
7. Implement more sophisticated priority calculation
8. Add error handling and retry logic
