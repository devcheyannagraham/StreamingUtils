# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

## [5.0.0] - 2026-09-23

### Breaking Changes
- The interceptor now applies `retry(...)` and `catchError(config.errorHandler)` to non-streaming requests too, not only requests marked with `STREAMING_RESPONSE`.
- `delayNotifier` now expects errors shaped as `Partial<HttpErrorResponse>` and uses `error.status` to decide retry eligibility.
- `serverErrorCheck` now throws `HttpErrorResponse` (built from parsed in-band payload data) instead of throwing plain `Error` strings.
- `chunkTimeoutHandler` signature changed from an `Error`-argument style to the RxJS timeout fallback style: `(info: TimeoutInfo<HttpEvent<any>, unknown>) => Observable<never>`.

### Added
- Configurable default retryable status list now includes connection-level failures with `0` in addition to HTTP statuses `408, 429, 500, 502, 503, 504`.
- Exported `ExtendedHttpEvent` helper type for typed access to interceptor-attached `parsedData`.
- Expanded README coverage for retry policy behavior, in-band error handling, and chunk-timeout customization.

### Changed
- Default chunk-timeout fallback now throws an `HttpErrorResponse` with `status: 408` and human-readable text in `.error`.
- `streamSubscription(...).errorCB` now receives `Partial<HttpErrorResponse>` to preserve status-aware retry metadata.
- Timeout operator construction in the interceptor now uses explicit type narrowing for RxJS overload safety (removes unsafe `as any` casting).
- Retry decision flow is centralized in `delayNotifier`; non-retryable statuses are rethrown immediately.

### Migration Notes
- If you relied on non-streaming requests bypassing retry/error handling, set custom `retryConfig`/`errorHandler` for those calls.
- To opt out of retries for custom thrown errors, omit `status` or use a status not listed in `retryableErrorStatusCode`.
- Ensure custom thrown errors include `status` when you want retries to occur.
- Update custom timeout handlers to accept `TimeoutInfo<HttpEvent<any>, unknown>` and return `Observable<never>`.
