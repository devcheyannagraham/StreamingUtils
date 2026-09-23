# @chey.dev/streamingutils

Helpers for consuming and parsing newline-delimited JSON (NDJSON) streams with Angular's `HttpClient`, featuring built-in buffering, configurable chunk timeouts, and automatic retry handling for network, transient, and in-band server errors.

## Features

- **Automated NDJSON Parsing & Buffering:** Handles split chunks and merges parsed JSON objects onto incoming Angular `HttpEvent` objects as `event.parsedData` while preserving the original event stream for advanced consumers.
- **In-Band Error Detection:** Detects `{ "error": "..." }` server payloads sent during an active stream (where HTTP status is committed to 200) and throws them into the RxJS pipeline, where `delayNotifier` decides whether to retry.
- **Resilient Retry & Timeout Logic:** Configurable linear backoff with random jitter, chunk timeouts, and full compatibility with RxJS `RetryConfig` and `TimeoutConfig`. Retry (`RetryConfig`) applies to every intercepted request, streaming or not; chunk timeouts (`TimeoutConfig`) only apply to streaming requests.
- **Global & Request-Level Config:** Set app-wide defaults at bootstrap or customize behavior per request via `HttpContext`.
- **Consumer-Friendly Subscription Helper:** `streamSubscription<T>` is a convenience wrapper that types `event.parsedData` for HttpEventType == DownloadProgress. The full Angular event stream still flows through unchanged.
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

> **Note:** The interceptor operates globally and applies retry (`retryConfig`/`delayNotifier`) and `errorHandler` handling to every request it intercepts. Streaming-specific behavior — chunk buffering, `event.parsedData`, chunk timeouts, and in-band error detection via `serverErrorCheck` — only activates for requests explicitly marked with `STREAMING_RESPONSE`.

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

`streamSubscription` also supports `completeCB`, which runs when the request observable completes. Use it alongside `nextCB` and `errorCB` when you need to respond to the stream's events.

### Raw Angular events are still available

This package does not replace Angular's event stream. It adds buffered NDJSON parsing by attaching `parsedData` to each event while leaving the original `HttpEvent` available to the subscriber.

If you prefer to handle the raw Angular stream yourself, skip `streamSubscription` and subscribe directly to `HttpClient` with `observe: "events"` and `responseType: "text"`.

For more on Angular's native streaming APIs, see the official docs: https://v21.angular.dev/guide/http/making-requests

---

### 3. Non-Streaming Requests Are Retried Too

`HTTPStreamInterceptor` also applies retry and error handling to regular, non-streaming requests via `retryConfig`, `delayNotifier` and `errorHandler`.

```ts
import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";

@Injectable({ providedIn: "root" })
export class UserService {
  constructor(private readonly http: HttpClient) {}

  getUser(id: string) {
    // No STREAMING_RESPONSE context needed — this plain request still
    // retries automatically on transient failures (network errors, 5xx, etc.).
    return this.http.get(`/api/users/${id}`);
  }
}
```

You can still customize `maxRetryCount`, `delay`, or `retryableErrorStatusCode` for these requests, either globally via `setDefaultConfig` or per request via `STREAM_CONFIG` — `STREAMING_RESPONSE` is not required to use `STREAM_CONFIG`.

```ts
import { HttpClient, HttpContext } from "@angular/common/http";
import { STREAM_CONFIG } from "@chey.dev/streamingutils/globals";

// Retry up to 5 times with a 2s base delay for this request only.
this.http.get("/api/users/123", {
  context: new HttpContext().set(STREAM_CONFIG, {
    maxRetryCount: 5,
    delay: 2_000,
    retryableErrorStatusCode: [429, 503],
  }),
});
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

console.log(getDefaultConfig());
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

| Option                     | Type                                                                | Default                             | Description                                                                                                                           |
| :------------------------- | :------------------------------------------------------------------ | :---------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------ |
| `maxRetryCount`            | `number`                                                            | `3`                                 | Maximum number of retry attempts before giving up. Setting to `0` disables retries completely.                                        |
| `delay`                    | `number`                                                            | `1000`                              | Base delay between retries in milliseconds.                                                                                           |
| `maxDelay`                 | `number`                                                            | `5000`                              | Maximum calculated retry delay ceiling in milliseconds.                                                                               |
| `jitter`                   | `number`                                                            | `500`                               | Maximum random jitter in milliseconds added to retry delay.                                                                           |
| `chunkTimeout`             | `number`                                                            | `7000`                              | Timeout in milliseconds between consecutive streamed chunks.                                                                          |
| `resetOnSuccess`           | `boolean`                                                           | `true`                              | Reset retry count on successful emission (RxJS `retry` option).                                                                       |
| `retryConfig`              | `RetryConfig \| null`                                               | `null`                              | Custom RxJS `RetryConfig` object to bypass default backoff calculation.                                                               |
| `retryableErrorStatusCode` | `any[]`                                                             | `[0, 408, 429, 500, 502, 503, 504]` | HTTP status codes treated as retryable by `delayNotifier`, applied uniformly to transport errors, chunk timeouts, and in-band errors. |
| `timeoutConfig`            | `number \| Date \| TimeoutConfig \| null`                           | `null`                              | Custom duration, Date, or RxJS `TimeoutConfig` object.                                                                                |
| `delayNotifier`            | `DelayNotifierType`                                                 | _linear backoff_                    | Factory function that decides retry eligibility (via `retryableErrorStatusCode`) and produces the retry delay timer Observable.       |
| `errorHandler`             | `(error: any, caught: Observable<any>) => ObservableInput<any>`     | _rethrow_                           | Terminal error handler passed to RxJS `catchError`; final catcher for errors that terminate this request pipeline.                    |
| `serverErrorCheck`         | `(event: HttpEvent<any>) => HttpEvent<any>`                         | _in-band check_                     | Detects in-band `{ error: "..." }` payloads and throws unconditionally; `delayNotifier` decides whether the thrown error is retried.  |
| `chunkTimeoutHandler`      | `(info: TimeoutInfo<HttpEvent<any>, unknown>) => Observable<never>` | _rethrow_                           | Fallback error handler triggered when chunk timeout occurs.                                                                           |

### Error Handling

#### errorHandler

`errorHandler` is an optional configuration setting passed directly to RxJS `catchError`. It acts as the final catcher for errors that terminate this specific request pipeline. By default it rethrows the final error, which then reaches the subscription via `errorCB` in `streamSubscription`. If you need different behavior, provide your own RxJS-compatible handler function through `setDefaultConfig` or `STREAM_CONFIG`.

A compatible handler follows the RxJS `catchError` contract:

```ts
const customErrorHandler = (error: any, caught: Observable<any>) => {
  console.error("Stream failed:", error);
  return throwError(() => error);
};
```

#### StreamSubscription: errorCB

Use `errorCB` for application-specific error handling after the stream has failed:

```ts
import { streamSubscription } from "@chey.dev/streamingutils/globals";

streamSubscription({
  errorCB: (error) => {
    const message =
      error?.status != null
        ? `Request failed with status ${error.status}`
        : (error?.message ?? "The stream failed");

    console.error(message, error);
    // Update application state, show a notification, or start a fallback flow.
  },
});
```

### Customizing Retry & Timeout Values

> **Tip:** To disable retries entirely on a streaming request, set `maxRetryCount: 0`.

#### Per-Request:

```ts
import { HttpClient, HttpContext } from "@angular/common/http";
import {
  STREAMING_RESPONSE,
  STREAM_CONFIG,
} from "@chey.dev/streamingutils/globals";

// Customizes retry timing for this specific request only
this.http.post(
  "/api/stream",
  {},
  {
    context: new HttpContext()
      .set(STREAMING_RESPONSE, true)
      .set(STREAM_CONFIG, {
        maxRetryCount: 5, // Retry up to 5 times (default is 3)
        delay: 2_000, // Base delay of 2s between retries (default is 1s)
        maxDelay: 10_000, // Cap maximum delay at 10s (default is 5s)
        jitter: 250, // Random jitter up to 250ms (default is 500ms)
        chunkTimeout: 10_000, // Wait up to 10s between chunks (default is 7s)
      }),
    reportProgress: true,
    observe: "events",
    responseType: "text",
  },
);
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
  retryableErrorStatusCode,
}: {
  delay: number;
  maxDelay: number;
  jitter: number;
  maxRetryCount: number;
  retryableErrorStatusCode: any[];
}) => (
  error: Partial<HttpErrorResponse>,
  retryCount?: number,
) => Observable<any>;
```

1. The **outer function** receives the configured timing settings (`delay`, `maxDelay`, `jitter`, `maxRetryCount`, `retryableErrorStatusCode`).
2. The **returned inner function** is passed to RxJS `retry({ delay: ... })`. It receives the thrown `error` and the current `retryCount` (1-indexed attempt number). This is the single place responsible for deciding whether ANY error — a real HTTP failure, a chunk timeout, or an in-band error thrown by `serverErrorCheck` — should be retried.
   - Return `timer(ms)` to wait and schedule the next retry attempt.
   - Return `throwError(() => error)` or throw an error to immediately abort retries and forward to `errorCB`.

#### Default Delay Notifier

The default delay notifier uses `DEFAULT_RETRYABLE_ERROR_STATUS_CODES` (`[0, 408, 429, 500, 502, 503, 504]`) to determine which errors are retried. By default only error codes in the status codes array are retried.

##### Streamed Error:

` res.write({"error":"db error", "status": 429, "message": "too many requests"})`.

`error` key in object catches and `status:429` triggers retry

##### Non-Streamed Error response:

`res.status(429).json({{"error":"db error", "status": 429, "message": "too many requests"}})`

`res.status(429)` catches and triggers retry. Everything in the object is ignored.

You can supply your own notifier if you need different timing or eligibility rules.

#### Example 1: Custom Delay Notifier: Constant Fixed Delay

```ts
import { timer } from "rxjs";
import { type DelayNotifierType } from "@chey.dev/streamingutils/globals";

// Retries every 2 seconds regardless of attempt count or status codes
const fixedDelayNotifier: DelayNotifierType = () => {
  return (error, retryCount) => {
    console.log(`Retrying attempt #${retryCount} after 2000ms`);
    return timer(2000);
  };
};
```

#### Example 2: Custom Delay Notifier: Exponential Backoff with Max Ceiling

```ts
import { throwError, timer } from "rxjs";
import { type DelayNotifierType } from "@chey.dev/streamingutils/globals";

// Exponential backoff: 1s, 2s, 4s, 8s... capped at maxDelay
const exponentialBackoff: DelayNotifierType = ({ delay, maxDelay }) => {
  return (error, retryCount = 1) => {
    const calculatedDelay = Math.min(
      delay * Math.pow(2, retryCount - 1),
      maxDelay,
    );

    console.warn(
      `Retry attempt #${retryCount} scheduled in ${calculatedDelay}ms`,
    );
    return timer(calculatedDelay);
  };
};
```

#### Example 3: Custom Delay Notifier: Selective Retries by Error Type

```ts
import { throwError, timer } from "rxjs";
import { type DelayNotifierType } from "@chey.dev/streamingutils/globals";

// Only retry for server errors, abort immediately for unauthorized or client errors
const selectiveRetryNotifier: DelayNotifierType = ({ delay }) => {
  return (error, retryCount = 1) => {
    // Abort retry if error message indicates an unrecoverable client issue
    if (
      error?.message?.includes("Unauthorized") ||
      error?.message?.includes("403")
    ) {
      return throwError(
        () => new Error("Aborting retries due to authentication failure"),
      );
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

By default, `serverErrorCheck` inspects each `DownloadProgress` event's `event.parsedData` for an `error` property (i.e. `{ error: string, status?: number }`). When found, it wraps the payload in `new HttpErrorResponse(event.parsedData)`, using `parsedData.status` as the HTTP status and `parsedData.error` as the resulting `.error` value. Retry eligibility is decided afterward, centrally, by `delayNotifier`, which checks the thrown value's `status` against `retryableErrorStatusCode`.

```ts
{"error": "custom error"}
```

```ts
{"error": "custom error", "status": 404, "message": "page not found"}
```

If you don't want a particular in-band error to trigger a retry, give it a `status` that is NOT in `retryableErrorStatusCode` or omit the status field.

#### Customizing `serverErrorCheck`

You can customize `serverErrorCheck` if your backend uses a different JSON structure, nested failure codes, status flags, or custom error envelopes.

#### Signature

```ts
serverErrorCheck: (event: HttpEvent<any>) => HttpEvent<any>;
```

#### Expected thrown error shape

`delayNotifier` is typed around `Partial<HttpErrorResponse>`, so the expected thrown error type is `HttpErrorResponse` or an object with the same key fields used by retry logic (`status`, plus readable text in `error`).

`message` may still exist on thrown errors, but the preferred location for custom human-readable details is the `error` field.

If `serverErrorCheck` throws a plain object instead of an error instance, retries can still work when `status` is present, but you lose normal error semantics (for example stack traces and `instanceof` checks).

#### Example 1: Custom Property Name or Status Flag

If your API returns `{ status: "failed", failureReason: "..." }` instead of `{ error: "..." }`:

**Important**: Be sure the custom status is in `retryableErrorStatusCode`.

```ts
import { HttpClient, HttpContext } from "@angular/common/http";
import {
  STREAMING_RESPONSE,
  STREAM_CONFIG,
} from "@chey.dev/streamingutils/globals";

this.http.post(
  "/api/stream",
  {},
  {
    context: new HttpContext()
      .set(STREAMING_RESPONSE, true)
      .set(STREAM_CONFIG, {
        retryableErrorStatusCode: [0, 408, 429, 500, 502, 503, 504, "failed"],
      }),
    reportProgress: true,
    observe: "events",
    responseType: "text",
  },
);
```

```ts
import { HttpEvent, HttpEventType } from "@angular/common/http";

const customErrorCheck = (event: HttpEvent<any>): HttpEvent<any> => {
  if (event.type === HttpEventType.DownloadProgress) {
    const data = (event as any)?.parsedData;
    // catching failure status
    if (data?.status === "failed") {
      throw new Error(data.failureReason || "Streaming operation failed");
    }
  }
  return event;
};
```

#### Example 2: Nested Error Object with Error Codes

If your API wraps error details in an object like `{ error: { code: 4001, message: "..." } }`, attach the code as `status` on the thrown error so `delayNotifier` can evaluate it against `retryableErrorStatusCode`:

```ts
import {
  HttpEvent,
  HttpEventType,
  HttpErrorResponse,
} from "@angular/common/http";

enum DbErrorCode {
  Disconnected = 4001,
  QueryTimeout = 4002,
}

const nestedErrorCheck = (event: HttpEvent<any>): HttpEvent<any> => {
  if (event.type === HttpEventType.DownloadProgress) {
    const errorDetails = (event as any)?.parsedData?.error;
    if (errorDetails) {
      const message =
        typeof errorDetails === "string"
          ? errorDetails
          : `[${errorDetails.code}] ${errorDetails.message}`;

      throw new HttpErrorResponse({
        error: message,
        status: errorDetails.code,
      });
    }
  }
  return event;
};
```

You can supply your custom checker globally via `setDefaultConfig({ serverErrorCheck: customErrorCheck })` or per-request via `STREAM_CONFIG`.

---

### Retryable Error Status Codes (`retryableErrorStatusCode`)

`retryableErrorStatusCode` is the single policy `delayNotifier` uses to decide whether **any** error, a real transport failure, a chunk timeout, or an in-band error thrown by `serverErrorCheck`, should trigger a retry. By default it is `[0, 408, 429, 500, 502, 503, 504]` (status `0` covers connection-level failures like no HTTP response received). Errors without a `status` property or a status value outside this list are **not** retried. They immediately abort and are forwarded to `errorHandler` and then `error/errorCB` in the Observable subscription.

#### Customizing Globally

```ts
import {
  setDefaultRetryableErrorStatusCodes,
  getDefaultRetryableErrorCodes,
} from "@chey.dev/streamingutils/globals";

// Only retry on 429 and 503 app-wide
setDefaultRetryableErrorStatusCodes([429, 503]);

const currentCodes = getDefaultRetryableErrorCodes();
```

#### Customizing Per-Request

```ts
import { HttpClient, HttpContext } from "@angular/common/http";
import {
  STREAMING_RESPONSE,
  STREAM_CONFIG,
} from "@chey.dev/streamingutils/globals";

this.http.post(
  "/api/stream",
  {},
  {
    context: new HttpContext()
      .set(STREAMING_RESPONSE, true)
      .set(STREAM_CONFIG, {
        retryableErrorStatusCode: [429, 503],
      }),
    reportProgress: true,
    observe: "events",
    responseType: "text",
  },
);
```

---

### Custom Chunk Timeout Handling (`chunkTimeoutHandler`)

When a streaming request is active, `chunkTimeout` governs the maximum allowable wait time (in milliseconds) between consecutive data chunks. If no new chunk arrives within that period, the timeout triggers.

`chunkTimeoutHandler` corresponds directly to the fallback function that is passed to RxJS's `timeout({ with: ... })`. It provides a convenient way to customize just the timeout error behavior.

#### Default Behavior

By default, when a chunk timeout occurs, `chunkTimeoutHandler` creates an `HttpErrorResponse` with status `408` via:

```ts
() =>
  throwError(
    () =>
      new HttpErrorResponse({
        status: 408,
        error: "Chunk Request timed out",
      }),
  );
```

This error enters the RxJS pipeline and is handled by `delayNotifier` like other request errors.

#### Customizing `chunkTimeoutHandler`

You can provide a custom `chunkTimeoutHandler` if you want to customize how the timeout error is generated or logged. Refer to rxjs `timeout({with:})` documentation. https://rxjs.dev/api/operators/timeout

#### Signature

```ts
chunkTimeoutHandler: (info: TimeoutInfo<HttpEvent<any>, unknown>) =>
  Observable<never>;
```

#### Example 1: Custom Error Message and Diagnostics

```ts
import { HttpErrorResponse } from "@angular/common/http";
import { throwError, type Observable } from "rxjs";

const customTimeoutHandler = (info): Observable<never> => {
  console.warn("Chunk timeout encountered after emissions:", info.seen);
  return throwError(
    () =>
      new HttpErrorResponse({
        status: 408,
        error: "Data stream stalled: No response chunk received in time",
      }),
  );
};
```

#### Example 2: Setting `chunkTimeoutHandler` in Configuration

```ts
import { HttpErrorResponse } from "@angular/common/http";
import { HttpClient, HttpContext } from "@angular/common/http";
import { throwError } from "rxjs";
import {
  STREAMING_RESPONSE,
  STREAM_CONFIG,
  setDefaultConfig,
} from "@chey.dev/streamingutils/globals";

// Globally:
setDefaultConfig({
  chunkTimeout: 10_000,
  chunkTimeoutHandler: () =>
    throwError(
      () =>
        new HttpErrorResponse({
          status: 408,
          error: "Stream stalled",
        }),
    ),
});

// Or Per-Request:
this.http.post(
  "/api/stream",
  {},
  {
    context: new HttpContext()
      .set(STREAMING_RESPONSE, true)
      .set(STREAM_CONFIG, {
        chunkTimeout: 5_000,
        chunkTimeoutHandler: customTimeoutHandler,
      }),
    reportProgress: true,
    observe: "events",
    responseType: "text",
  },
);
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
      each: 3_000, // Timeout between subsequent chunks
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
Default is false.

```ts
import { setDebug } from "@chey.dev/streamingutils/globals";

// Enable debug logs for troubleshooting during development
setDebug(true);

```

When enabled, logs prefixed with `HSI:` will output request lifecycle events, chunk slicing, backoff timing, and in-band error detection.

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

The interceptor detects `{ error: string }` and throws it into the RxJS pipeline, where `delayNotifier` decides whether to retry based on `retryableErrorStatusCode`.

---

## API Summary

| Export                                | Module                  | Description                                                                                  |
| :------------------------------------ | :---------------------- | :------------------------------------------------------------------------------------------- |
| `HTTPStreamInterceptor`               | `HttpStreamInterceptor` | Functional Angular HTTP interceptor for parsing, retrying, and chunk-timing NDJSON requests. |
| `STREAMING_RESPONSE`                  | `globals`               | `HttpContextToken<boolean>` enabling streaming interceptor mechanics on a request.           |
| `STREAM_CONFIG`                       | `globals`               | `HttpContextToken<Partial<StreamConfig>>` for request-level configuration overrides.         |
| `setDefaultConfig`                    | `globals`               | Sets application-wide default `StreamConfig`.                                                |
| `getDefaultConfig`                    | `globals`               | Returns a clone of the current default `StreamConfig`.                                       |
| `streamSubscription<T>`               | `globals`               | Observer factory delivering parsed stream payloads of type `T` to callbacks.                 |
| `setDebug`                            | `globals`               | Enables or disables `HSI:` debug logging.                                                    |
| `StreamConfig`                        | `globals`               | Configuration interface for retry, timeout, backoff, and error handling.                     |
| `DelayNotifierType`                   | `globals`               | Higher-order factory function type for generating custom RxJS retry delay notifiers.         |
| `setDefaultRetryableErrorStatusCodes` | `globals`               | Overrides the module-level default retryable HTTP status codes used by `delayNotifier`.      |
| `getDefaultRetryableErrorCodes`       | `globals`               | Returns a clone of the current default retryable HTTP status codes.                          |

---

## License

ISC
