# @chey.dev/streamingutils

Helpers for consuming and parsing newline-delimited JSON (NDJSON) streams with Angular's `HttpClient`, featuring built-in buffering, configurable chunk timeouts, and automatic retry handling for network, transient, and in-band server errors.

## Features

- **Automated NDJSON Parsing & Buffering:** Handles split chunks and merges parsed JSON objects onto incoming Angular `HttpEvent` objects (`event.parsedData`).
- **In-Band Error Detection:** Detects `{ "error": "..." }` server payloads sent during an active stream (where HTTP status is committed to 200) and routes them into RxJS retry pipelines.
- **Resilient Retry & Timeout Logic:** Configurable linear backoff with random jitter, chunk timeouts, and full compatibility with RxJS `RetryConfig` and `TimeoutConfig`.
- **Global & Request-Level Config:** Set app-wide defaults at bootstrap or customize behavior per request via `HttpContext`.
- **Consumer-Friendly Subscription Helper:** `streamSubscription<T>` parses and delivers typed data payloads directly to callbacks.
- **Debug Mode:** Optional debug logging prefixed with `HSI:` for easy troubleshooting.

## Requirements

- Angular (`@angular/common` and `@angular/core`) `^21.1.0`
- RxJS `~7.8.0`

## Installation

```bash
npm install @chey.dev/streamingutils
```

---

## Setup

### 1. Register the Interceptor

Add `HTTPStreamInterceptor` to your application's `provideHttpClient` configuration (typically in `app.config.ts`):

```ts
import { ApplicationConfig } from "@angular/core";
import { provideHttpClient, withInterceptors } from "@angular/common/http";
import { HTTPStreamInterceptor } from "@chey.dev/streamingutils/HttpStreamInterceptor";

export const appConfig: ApplicationConfig = {
  providers: [provideHttpClient(withInterceptors([HTTPStreamInterceptor]))],
};
```

> **Note:** The interceptor operates globally, but streaming buffering, timeouts, and retry logic only activate for requests explicitly marked with `STREAMING_RESPONSE`. Non-streaming requests pass through unmodified.

---

### 2. Make a Streaming Request

To stream a request:
1. Set the `STREAMING_RESPONSE` context token to `true`.
2. Set `reportProgress: true`, `observe: 'events'`, and `responseType: 'text'` on Angular's `HttpClient` options.
3. Subscribe using `streamSubscription<T>`.

```ts
import { Injectable } from "@angular/core";
import { HttpClient, HttpContext, HttpEventType } from "@angular/common/http";
import {
  STREAMING_RESPONSE,
  streamSubscription,
} from "@chey.dev/streamingutils/globals";

// Shape of each NDJSON message from the server
interface StreamPayload {
  message?: string;
  count?: number;
  done?: boolean;
}

@Injectable({ providedIn: "root" })
export class StreamService {
  constructor(private readonly http: HttpClient) {}

  streamData() {
    return this.http
      .post(
        "/api/stream",
        {},
        {
          context: new HttpContext().set(STREAMING_RESPONSE, true),
          reportProgress: true,
          observe: "events",
          responseType: "text",
        },
      )
      .subscribe(
        streamSubscription<StreamPayload>({
          nextCB: (data, event) => {
            if (data?.message) {
              console.log("Chunk received:", data.message);
            }
            if (data?.done) {
              console.log("Stream completed");
            }
            if (event?.type === HttpEventType.Response) {
              console.log("Final HTTP response received");
            }
          },
          errorCB: (error) => {
            console.error("Stream failed after retries exhausted:", error);
          },
          completeCB: () => {
            console.log("Stream request finished");
          },
        }),
      );
  }
}
```

---

## Configuration

### Global Configuration

Set application-wide defaults once during bootstrap using `setDefaultConfig`:

```ts
import { setDefaultConfig } from "@chey.dev/streamingutils/globals";

setDefaultConfig({
  maxRetryCount: 5,
  chunkTimeout: 10_000,
  delay: 1_500,
  maxDelay: 10_000,
});
```

You can inspect the active defaults at any time using `getDefaultConfig`:

```ts
import { getDefaultConfig } from "@chey.dev/streamingutils/globals";

const currentDefaults = getDefaultConfig();
```

---

### Per-Request Configuration

Override settings for an individual request using the `STREAM_CONFIG` context token:

```ts
import { HttpClient, HttpContext } from "@angular/common/http";
import {
  STREAMING_RESPONSE,
  STREAM_CONFIG,
  streamSubscription,
} from "@chey.dev/streamingutils/globals";

this.http
  .post(
    "/api/stream",
    {},
    {
      context: new HttpContext()
        .set(STREAMING_RESPONSE, true)
        .set(STREAM_CONFIG, {
          maxRetryCount: 5,
          chunkTimeout: 12_000,
          delay: 2_000,
          maxDelay: 15_000,
        }),
      reportProgress: true,
      observe: "events",
      responseType: "text",
    },
  )
  .subscribe(streamSubscription({ ... }));
```

---

### Configuration Options (`StreamConfig`)

| Option | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `maxRetryCount` | `number` | `3` | Maximum number of retry attempts before giving up. |
| `delay` | `number` | `1000` | Base delay between retries in milliseconds. |
| `maxDelay` | `number` | `5000` | Maximum calculated retry delay ceiling in milliseconds. |
| `jitter` | `number` | `500` | Maximum random jitter in milliseconds added to retry delay. |
| `chunkTimeout` | `number` | `7000` | Timeout in milliseconds between consecutive streamed chunks. |
| `resetOnSuccess` | `boolean` | `undefined` | Reset retry count on successful emission (RxJS `retry` option). |
| `retryConfig` | `RetryConfig \| null` | `null` | Custom RxJS `RetryConfig` object to bypass default backoff calculation. |
| `timeoutConfig` | `number \| Date \| TimeoutConfig \| null` | `null` | Custom duration, Date, or RxJS `TimeoutConfig` object. |
| `delayNotifier` | `DelayNotifierType` | *linear backoff* | Factory function producing the retry delay timer Observable. |
| `errorHandler` | `(error: Error) => Observable<never>` | *rethrow* | Terminal error handler called when retries are exhausted. |
| `serverErrorCheck` | `(event: HttpEvent<any>) => HttpEvent<any>` | *in-band check* | Checks chunk payload for in-band `{ error: "..." }` markers. |
| `chunkTimeoutHandler` | `(error: Error) => Observable<never>` | *rethrow* | Fallback error handler triggered when chunk timeout occurs. |

---

### Advanced RxJS Customization

You can supply full RxJS `retryConfig` or `timeoutConfig` objects directly:

```ts
import { HttpContext } from "@angular/common/http";
import { throwError } from "rxjs";
import {
  STREAMING_RESPONSE,
  STREAM_CONFIG,
} from "@chey.dev/streamingutils/globals";

const context = new HttpContext()
  .set(STREAMING_RESPONSE, true)
  .set(STREAM_CONFIG, {
    // Custom RxJS timeout configuration
    timeoutConfig: {
      first: 5_000, // Timeout for the first chunk
      each: 3_000,  // Timeout between subsequent chunks
      with: (info) => throwError(() => new Error("Custom timeout exceeded")),
    },
    // Custom RxJS retry configuration
    retryConfig: {
      count: 4,
      resetOnSuccess: true,
    },
  });
```

---

## Debugging

Enable or disable `HSI:` console logs by importing the reactive `debug` signal and setting its value:

```ts
import { debug } from "@chey.dev/streamingutils/globals";

// Enable debug logs for troubleshooting during development
debug.set(true);

// Disable debug logs
debug.set(false);
```

When enabled (default: `false`), logs prefixed with `HSI:` will output request lifecycle events, chunk slicing, backoff timing, and in-band error detection.

---

## Server Protocol (NDJSON)

The backend stream should send newline-delimited (`\n`) JSON strings with appropriate streaming headers (`Content-Type: application/x-ndjson`).

### Standard Chunk Output
```text
{"message":"Processing step 1"}\n
{"message":"Processing step 2"}\n
{"done":true}\n
```

### In-Band Server Errors
Because streaming responses commit to an HTTP 200 header before body processing completes, the server can signal a mid-stream failure by writing an error object:

```json
{"error":"Upstream database unavailable"}\n
```

The interceptor detects `{ error: string }`, throws an error internally, and triggers your configured retry logic automatically.

---

## API Summary

| Export | Module | Description |
| :--- | :--- | :--- |
| `HTTPStreamInterceptor` | `HttpStreamInterceptor` | Functional Angular HTTP interceptor for parsing, retrying, and chunk-timing NDJSON requests. |
| `STREAMING_RESPONSE` | `globals` | `HttpContextToken<boolean>` enabling streaming interceptor mechanics on a request. |
| `STREAM_CONFIG` | `globals` | `HttpContextToken<Partial<StreamConfig>>` for request-level configuration overrides. |
| `setDefaultConfig` | `globals` | Sets application-wide default `StreamConfig`. |
| `getDefaultConfig` | `globals` | Returns a clone of the current default `StreamConfig`. |
| `streamSubscription<T>` | `globals` | Observer factory delivering parsed stream payloads of type `T` to callbacks. |
| `debug` | `globals` | `WritableSignal<boolean>` toggle controlling `HSI:` debug logging to the console. |
| `StreamConfig` | `globals` | Configuration interface for retry, timeout, backoff, and error handling. |

---

## License

ISC
