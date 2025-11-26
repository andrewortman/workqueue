# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project adheres to Semantic Versioning.

## [0.2.0] - 2025-11-26

### Changed
- `NewInMemory` now accepts variadic `Option` functions instead of a `TimeProvider` parameter.

### Added
- `WithTimeProvider(TimeProvider)` option to inject a custom time provider.
- `WithCapacity(int)` option to limit queue size.
- `ErrAtCapacity` error returned when queue is at capacity and cannot accept new items.

## [0.1.0] - 2025-10-11

### Added
- Initial public release of `workqueue`.
- In-memory queue with priority, delay, and expiry support.
- Goroutine-safe design for concurrent producers/consumers.
- Public API: `Put`, `Update`, `PutOrUpdate`, `UpdateConditional`, `PutOrUpdateConditional`, `Remove`, `Take`, `TakeMany`, `Size`.
- Performance documentation (Big-O) and separate Benchmarks section.
- Context-aware blocking operations and cancellation.
- Benchmarks and documentation.

[0.2.0]: https://github.com/andrewortman/workqueue/releases/tag/v0.2.0
[0.1.0]: https://github.com/andrewortman/workqueue/releases/tag/v0.1.0
