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

`streamSubscription` also supports `completeCB`, which runs when the request observable completes. Use it alongside `nextCB` and `errorCB` when you need to respond to the stream's completion event.

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
| `maxRetryCount` | `number` | `3` | Maximum number of retry attempts before giving up. Setting to `0` disables retries completely. |
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

### Error Handling

`errorHandler` is an optional configuration setting passed directly to RxJS `catchError`. After retries are exhausted, the default handler rethrows the final error so the subscriber receives it through `errorCB`, which `streamSubscription` forwards to the subscriber's error callback. If you need different behavior, supply your own RxJS-compatible handler function through `setDefaultConfig` or `STREAM_CONFIG`.

Use `errorCB` for application-specific error handling after the stream has failed:

```ts
import { HttpErrorResponse } from "@angular/common/http";
import { streamSubscription } from "@chey.dev/streamingutils/globals";

streamSubscription({
  errorCB: (error) => {
    const message = error instanceof HttpErrorResponse
      ? `Request failed with status ${error.status}`
      : error?.message ?? "The stream failed";

    console.error(message, error);
    // Update application state, show a notification, or start a fallback flow.
  },
});
```

### Customizing Retry & Timeout Values (Without Writing Custom Objects)

If you only want to change timing values (retry count, delay duration, chunk timeouts) without writing custom RxJS retry objects or functions, simply pass the individual properties:

> **Tip:** To disable retries entirely on a streaming request, set `maxRetryCount: 0`.

#### Per-Request:
```ts
import { HttpClient, HttpContext } from "@angular/common/http";
import { STREAMING_RESPONSE, STREAM_CONFIG } from "@chey.dev/streamingutils/globals";

// Customizes retry timing for this specific request only
this.http.post("/api/stream", {}, {
  context: new HttpContext()
    .set(STREAMING_RESPONSE, true)
    .set(STREAM_CONFIG, {
      maxRetryCount: 5,     // Retry up to 5 times (default is 3)
      delay: 2_000,         // Base delay of 2s between retries (default is 1s)
      maxDelay: 10_000,     // Cap maximum delay at 10s (default is 5s)
      jitter: 250,          // Random jitter up to 250ms (default is 500ms)
      chunkTimeout: 10_000, // Wait up to 10s between chunks (default is 7s)
    }),
  reportProgress: true,
  observe: "events",
  responseType: "text",
});
```

#### Globally for the Entire Application:
```ts
import { setDefaultConfig } from "@chey.dev/streamingutils/globals";

// Adjusts defaults across all streaming requests in the app
setDefaultConfig({
  maxRetryCount: 5,
  delay: 2_000,
  maxDelay: 10_000,
  chunkTimeout: 10_000,
});
```

The interceptor will automatically construct the RxJS retry and timeout pipelines using your values.

---

### Custom Retry Delays (`DelayNotifierType`)

`DelayNotifierType` is a higher-order factory function used to customize retry timing and retry conditions.

#### Signature
```ts
export type DelayNotifierType = ({
  delay,
  maxDelay,
  jitter,
  maxRetryCount,
}: {
  delay: number;
  maxDelay: number;
  jitter: number;
  maxRetryCount: number;
}) => (error?: Error, retryCount?: number) => Observable<any>;
```

1. The **outer function** receives the configured timing settings (`delay`, `maxDelay`, `jitter`, `maxRetryCount`).
2. The **returned inner function** is passed to RxJS `retry({ delay: ... })`. It receives the thrown `error` and the current `retryCount` (1-indexed attempt number).
   - Return `timer(ms)` to wait and schedule the next retry attempt.
   - Return `throwError(() => error)` or throw an error to immediately abort retries.

#### Example 1: Constant Fixed Delay
```ts
import { timer } from "rxjs";
import { type DelayNotifierType } from "@chey.dev/streamingutils/globals";

// Retries every 2 seconds regardless of attempt count
const fixedDelayNotifier: DelayNotifierType = () => {
  return (error, retryCount) => {
    console.log(`Retrying attempt #${retryCount} after 2000ms`);
    return timer(2000);
  };
};
```

#### Example 2: Exponential Backoff with Max Ceiling
```ts
import { throwError, timer } from "rxjs";
import { type DelayNotifierType } from "@chey.dev/streamingutils/globals";

// Exponential backoff: 1s, 2s, 4s, 8s... capped at maxDelay
const exponentialBackoff: DelayNotifierType = ({ delay, maxDelay }) => {
  return (error, retryCount = 1) => {
    const calculatedDelay = Math.min(delay * Math.pow(2, retryCount - 1), maxDelay);

    console.warn(`Retry attempt #${retryCount} scheduled in ${calculatedDelay}ms`);
    return timer(calculatedDelay);
  };
};
```

#### Example 3: Selective Retries by Error Type
```ts
import { throwError, timer } from "rxjs";
import { type DelayNotifierType } from "@chey.dev/streamingutils/globals";

// Only retry for server errors, abort immediately for unauthorized or client errors
const selectiveRetryNotifier: DelayNotifierType = ({ delay }) => {
  return (error, retryCount = 1) => {
    // Abort retry if error message indicates an unrecoverable client issue
    if (error?.message?.includes("Unauthorized") || error?.message?.includes("403")) {
      return throwError(() => new Error("Aborting retries due to authentication failure"));
    }

    return timer(delay);
  };
};
```

You can pass your custom notifier globally via `setDefaultConfig({ delayNotifier: exponentialBackoff })` or per request via `STREAM_CONFIG`.

---

### Custom In-Band Error Checking (`serverErrorCheck`)

In-band errors are failures that occur mid-stream after the HTTP response status has already been committed to `200 OK`. Because status code headers cannot change once streaming begins, the server delivers the failure message as part of the body data chunk.

#### Default Behavior
By default, `serverErrorCheck` inspects each `DownloadProgress` event's `event.parsedData` for an `error` property (i.e. `{ error: string }`). If present, it throws a JavaScript `Error(error)` inside the RxJS pipeline, which automatically triggers your retry logic. If no error property exists, it passes the `HttpEvent` through unmodified.

#### Customizing `serverErrorCheck`
You can customize `serverErrorCheck` if your backend uses a different JSON structure, nested failure codes, status flags, or custom error envelopes.

> **Important:** Even if you change the parsing condition or property names your custom function checks for, these errors are still **in-band errors** occurring inside an established HTTP 200 stream. When your function throws an error, it is caught by the RxJS retry mechanism and handled identically to default in-band errors.

#### Signature
```ts
serverErrorCheck: (event: HttpEvent<any>) => HttpEvent<any>;
```

#### Example 1: Custom Property Name or Status Flag
If your API returns `{ status: "failed", failureReason: "..." }` instead of `{ error: "..." }`:

```ts
import { HttpEvent, HttpEventType } from "@angular/common/http";

const customErrorCheck = (event: HttpEvent<any>): HttpEvent<any> => {
  if (event.type === HttpEventType.DownloadProgress) {
    const data = (event as any)?.parsedData;
    if (data?.status === "failed") {
      throw new Error(data.failureReason || "Streaming operation failed");
    }
  }
  return event;
};
```

#### Example 2: Nested Error Object with Error Codes
If your API wraps error details in an object like `{ error: { code: "DB_DISCONNECTED", message: "..." } }`:

```ts
import { HttpEvent, HttpEventType } from "@angular/common/http";

const nestedErrorCheck = (event: HttpEvent<any>): HttpEvent<any> => {
  if (event.type === HttpEventType.DownloadProgress) {
    const errorDetails = (event as any)?.parsedData?.error;
    if (errorDetails) {
      const message = typeof errorDetails === "string" 
        ? errorDetails 
        : `[${errorDetails.code}] ${errorDetails.message}`;
      throw new Error(message);
    }
  }
  return event;
};
```

You can supply your custom checker globally via `setDefaultConfig({ serverErrorCheck: customErrorCheck })` or per-request via `STREAM_CONFIG`.

---

### Custom Chunk Timeout Handling (`chunkTimeoutHandler`)

When a streaming request is active, `chunkTimeout` governs the maximum allowable wait time (in milliseconds) between consecutive data chunks. If no new chunk arrives within that period, the timeout triggers.

`chunkTimeoutHandler` corresponds directly to the fallback function that is passed to RxJS's `timeout({ with: ... })`. It provides a convenient way to customize just the timeout error behavior without needing to construct an entire RxJS `timeoutConfig` object manually.

#### Default Behavior
By default, when a chunk timeout occurs, `chunkTimeoutHandler` creates a new error via:
```ts
() => throwError(() => new Error("Chunk Request timed out"))
```
This error enters the RxJS pipeline, alerting the retry mechanism (`delayNotifier`) that a chunk timed out and initiating a retry attempt up to `maxRetryCount`.

#### Customizing `chunkTimeoutHandler`
You can provide a custom `chunkTimeoutHandler` if you want to customize how the timeout error is generated or logged, without having to write a full `timeoutConfig` object.

#### Signature
```ts
chunkTimeoutHandler: (error: Error) => Observable<never>;
```

#### Example 1: Custom Error Message and Diagnostics
```ts
import { throwError, type Observable } from "rxjs";

const customTimeoutHandler = (error: Error): Observable<never> => {
  console.warn("Chunk timeout encountered:", error.message);
  return throwError(() => new Error("Data stream stalled: No response chunk received in time"));
};
```

#### Example 2: Setting `chunkTimeoutHandler` in Configuration
You can configure it globally or per-request:

```ts
import { HttpClient, HttpContext } from "@angular/common/http";
import {
  STREAMING_RESPONSE,
  STREAM_CONFIG,
  setDefaultConfig,
} from "@chey.dev/streamingutils/globals";

// Globally:
setDefaultConfig({
  chunkTimeout: 10_000,
  chunkTimeoutHandler: (err) => throwError(() => new Error(`Stream stalled: ${err.message}`)),
});

// Or Per-Request:
this.http.post("/api/stream", {}, {
  context: new HttpContext()
    .set(STREAMING_RESPONSE, true)
    .set(STREAM_CONFIG, {
      chunkTimeout: 5_000,
      chunkTimeoutHandler: customTimeoutHandler,
    }),
  reportProgress: true,
  observe: "events",
  responseType: "text",
});
```

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

Enable or disable `HSI:` console logs by importing `setDebug`:

```ts
import { setDebug } from "@chey.dev/streamingutils/globals";

// Enable debug logs for troubleshooting during development
setDebug(true);

// Disable debug logs
setDebug(false);
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
| `setDebug` | `globals` | Enables or disables `HSI:` debug logging. |
| `StreamConfig` | `globals` | Configuration interface for retry, timeout, backoff, and error handling. |
| `DelayNotifierType` | `globals` | Higher-order factory function type for generating custom RxJS retry delay notifiers. |

---

## License

ISC
