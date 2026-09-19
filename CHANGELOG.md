# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added
- `retryableError` helper to determine whether an in-band stream error status is retryable, using default retryable HTTP status codes (408, 429, 500, 502, 503, 504).

### Changed
- `serverErrorCheck` now only throws on in-band server errors whose status code is retryable, logging non-retryable status codes instead of throwing.
- `serverErrorCheck` and related types now use an `ExtendedHttpEvent`/`ExtendedError` shape instead of `@ts-ignore` casts for `parsedData`/`error` access.
