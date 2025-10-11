# Contributing to workqueue

Thank you for your interest in contributing! This guide explains how to propose changes and what we expect to maintain quality and velocity.

## Ground Rules

- Be respectful and constructive.
- Keep changes focused and incremental; large, mixed PRs are hard to review.
- Prefer discussion before implementation when behavior or API might change.

## How to Contribute

1. Open an issue before writing code
   - Describe the problem or proposal clearly
   - Include context, use cases, and constraints
   - Solicit feedback from the maintainer (@andrewortman)
   - Wait for alignment/approval before implementation
2. Fork the repo and create a topic branch from `main`
3. Implement the change with tests and documentation updates
4. Run the test suite locally and ensure lints pass
5. Open a pull request that references the issue and explains the approach

> Pull requests that alter the public API, behavior, or performance characteristics must have an approved issue first. PRs that would require a minor or major version bump without such an issue may be rejected.

## Versioning and SemVer Policy

This project follows Semantic Versioning (SemVer): `MAJOR.MINOR.PATCH`.

- Patch (x.y.Z): Backwards-compatible bug fixes only. No API or behavior changes beyond fixing a defect.
- Minor (x.Y.z): Backwards-compatible feature additions, improvements, or deprecations. No breaking changes.
- Major (X.y.z): Backwards-incompatible changes, removals, or redesigns.

Notes for pre-1.0 (`0.y.z`):
- Per SemVer, public API is not guaranteed to be stable before 1.0. However, we still treat the minor version (`0.Y.z`) as the feature cadence and the patch (`0.y.Z`) as bug fixes. Breaking changes in `0.y.z` may happen but should be avoided without a compelling reason and prior discussion.

Examples:
- `0.1.1` — bug fix, no API changes
- `0.2.0` — new features or improvements; avoid breaking changes
- `1.0.0` — first stable API; any breaking change thereafter requires a new major version

## When an Issue Is Required

An approved issue is required before submitting PRs that:
- Change or add public API (types, functions, behavior)
- Modify default behavior or semantics
- Introduce performance-affecting changes
- Deprecate or remove functionality

Small bug fixes, docs, tests, and internal refactors that do not change public API or behavior can be opened directly as PRs but are still welcome to start with an issue for visibility.

## Code Standards

- Format with `gofmt`; use `go vet` and `staticcheck`
- Add tests for new behavior and edge cases
- Keep functions small and clear; prefer early returns
- Avoid unnecessary concurrency; document concurrency invariants where used
- Update README and CHANGELOG when public API changes

## Commit Messages

Use clear, descriptive messages. Conventional commits are welcome:

```
feat: add delayed item promotion tracing
fix: prevent expired items from re-entering prio heap
docs: clarify TakeMany context behavior
test: add size accounting tests
refactor: isolate heap maintenance logic
```

## Pull Request Checklist

- [ ] Issue exists and is referenced (if required)
- [ ] Tests added/updated and passing (`go test ./...`)
- [ ] Lints/static analysis pass
- [ ] Documentation updated (README, examples, CHANGELOG if user-facing)
- [ ] PR description explains problem, approach, trade-offs

## Review and Merging

- PRs are reviewed by the maintainer (@andrewortman)
- Expect iteration; please respond to feedback promptly
- Squash-merge preferred to keep history clean

## Questions

Open an issue with your question or proposal. Thank you for helping make workqueue better!
