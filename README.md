# @chey.dev/streamingutils

Helpers for consuming newline-delimited JSON (NDJSON) streams from Angular `HttpClient` requests.

## What this package does

Normal HTTP responses are usually delivered after the complete body arrives. This package supports a long-lived `POST` request whose response is delivered as several JSON objects while the connection remains open.

It provides two pieces:

- `HTTPStreamInterceptor` buffers and parses newline-delimited JSON, detects in-band server errors, and retries failed streams.
- `streamSubscription<T>` creates a typed RxJS `Observer` that forwards each parsed payload to callbacks.

### Why errors are sent in the response body

Once a server sends response headers, the HTTP status is committed. A server cannot change a successful `200` response to `500` halfway through the body. The server therefore sends failures as a normal NDJSON message:

```json
{"error":"The stream failed"}
```

The interceptor detects that message and throws it inside the RxJS pipeline, where retry handling can process it.

## Requirements

- Angular `@angular/common` and `@angular/core` `21.1.0` or later
- RxJS `7.8.1`
- An HTTP server that writes one complete JSON object per line

## Installation

Install the package in your Angular application:

```bash
npm install @chey.dev/streamingutils
```

The package uses Angular and RxJS as peer dependencies. Your application must provide them.

## Setup

### 1. Register the interceptor once

Add the interceptor to the application's `provideHttpClient` configuration. With a standalone Angular application, this is usually `src/app/app.config.ts`:

```ts
import { ApplicationConfig } from '@angular/core';
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { HTTPStreamInterceptor } from '@chey.dev/streamingutils/HttpStreamInterceptor';

export const appConfig: ApplicationConfig = {
  providers: [
    provideHttpClient(withInterceptors([HTTPStreamInterceptor])),
  ],
};
```

Registration is global, but stream behavior only applies to requests marked with `STREAMING_RESPONSE`.

### 2. Mark a request as streamed

A streamed request must use:

- `STREAMING_RESPONSE: true` to enable stream handling
- `observe: 'events'` to expose progress events
- `reportProgress: true` to emit `DownloadProgress` events
- `responseType: 'text'` because the body contains NDJSON rather than one JSON document

```ts
import { HttpClient, HttpContext, HttpEventType } from '@angular/common/http';
import {
  STREAMING_RESPONSE,
  streamSubscription,
} from '@chey.dev/streamingutils/globals';

type StreamChunk = {
  responseData?: { message: string };
  done?: boolean;
  error?: string;
};

export class StreamService {
  constructor(private readonly http: HttpClient) {}

  startStream(): void {
    const context = new HttpContext().set(STREAMING_RESPONSE, true);

    this.http.post('/api/stream', {}, {
      context,
      observe: 'events',
      reportProgress: true,
      responseType: 'text',
    }).subscribe(
      streamSubscription<StreamChunk>({
        nextCB: (chunk, event) => {
          if (chunk?.responseData) {
            console.log('data:', chunk.responseData.message);
          }

          if (chunk?.done) {
            console.log('stream finished');
          }

          if (event?.type === HttpEventType.Response) {
            console.log('final HTTP response received');
          }
        },
        errorCB: (error) => console.error('stream failed:', error),
        completeCB: () => console.log('request complete'),
      }),
    );
  }
}
```

`streamSubscription<T>` makes the parsed value passed to `nextCB` a `T`. The generic should describe the JSON objects your server writes, not the raw text response. The second callback argument is the original Angular `HttpEvent`.

## Server response format

The server must write one complete JSON object followed by `\n` for every message:

```text
{"responseData":{"message":"first"}}\n
{"responseData":{"message":"second"}}\n
{"done":true}\n
```

An Express route can produce that response as follows:

```ts
app.post('/api/stream', async (_req, res) => {
  res.setHeader('Content-Type', 'application/x-ndjson');
  res.flushHeaders();

  res.write(JSON.stringify({
    responseData: { message: 'first' },
  }) + '\n');

  await new Promise((resolve) => setTimeout(resolve, 1000));

  res.write(JSON.stringify({
    responseData: { message: 'second' },
  }) + '\n');

  res.write(JSON.stringify({ done: true }) + '\n');
  res.end();
});
```

To signal a mid-stream failure, write an error object and end the response:

```ts
res.write(JSON.stringify({ error: 'The upstream service failed' }) + '\n');
res.end();
```

The error value must be a string. Serializing a JavaScript `Error` object directly usually produces `{}` because its `message` property is not enumerable.

## Per-request configuration

Retry and timeout settings are carried by `STREAM_CONFIG`, an Angular `HttpContextToken`. This makes configuration request-specific instead of changing shared module state.

The default values are:

| Setting | Default | Purpose |
| --- | ---: | --- |
| `delay` | `1000` | Base retry delay in milliseconds. |
| `maxDelay` | `15000` | Maximum calculated retry delay in milliseconds. |
| `jitter` | `500` | Maximum random jitter added to the retry delay. |
| `chunkTimeout` | `5000` | Maximum time between streamed chunks in milliseconds. |
| `requestTimeout` | `30000` | Request-level timeout value available to the caller. |
| `retryCount` | `3` | Number of retries after the initial request. |

When setting `STREAM_CONFIG`, provide the complete `StreamConfig` object. The token value replaces the default object; it does not deep-merge with it.

```ts
import { HttpContext } from '@angular/common/http';
import { throwError, timer } from 'rxjs';
import {
  STREAM_CONFIG,
  STREAMING_RESPONSE,
  type StreamConfig,
} from '@chey.dev/streamingutils/globals';

const config: StreamConfig = {
  delay: 2000,
  maxDelay: 30000,
  jitter: 500,
  chunkTimeout: 10000,
  requestTimeout: 30000,
  retryCount: 5,
  delayNotifier: ({ delay = 2000 }) => {
    return (_error, retryCount) => timer(retryCount * delay);
  },
  errorHandler: (error) => throwError(() => error),
  serverErrorCheck: (event) => event,
};

const context = new HttpContext()
  .set(STREAMING_RESPONSE, true)
  .set(STREAM_CONFIG, config);
```

### Custom delay notifier

`DelayNotifierType` is a factory. It receives delay settings and returns the callback that RxJS invokes for each retry:

```ts
type DelayNotifierType = (settings: {
  delay?: number;
  maxDelay?: number;
  jitter?: number;
}) => (
  error?: Error,
  retryCount?: number,
) => Observable<unknown>;
```

For example, a fixed one-second retry delay is:

```ts
const fixedDelay: DelayNotifierType = () => () => timer(1000);
```

Returning `timer(...)` schedules another attempt. Returning `throwError(() => error)` stops retrying.

## Non-streaming requests

The interceptor passes ordinary requests through without stream parsing or retries. Do not set `STREAMING_RESPONSE` for normal requests:

```ts
this.http.get('/api/status').subscribe({
  next: (status) => console.log(status),
  error: (error) => console.error(error),
});
```

## Troubleshooting

### No chunks arrive

Verify `observe: 'events'`, `reportProgress: true`, and `responseType: 'text'`. Also verify that the server writes data progressively instead of building the entire response before calling `res.end()`.

### Chunks are not parsed

Every JSON object must be valid on its own and end with a newline. Do not send a JSON array, pretty-printed JSON, or Server-Sent Events `data:` prefixes.

### The request retries unexpectedly

The interceptor retries when the server sends `{ "error": "..." }`, when the connection fails, or when no chunk arrives before `chunkTimeout`. Check the server logs and the request's `STREAM_CONFIG` values.

### The stream remains open after a component is destroyed

Keep the `Subscription` returned by `subscribe()` and call `unsubscribe()` during component or service teardown. Unsubscribing cancels the client-side request.

## API

| Export | Module | Description |
| --- | --- | --- |
| `HTTPStreamInterceptor` | `HttpStreamInterceptor` | Functional Angular interceptor for parsing, retrying, and timing out marked stream requests. |
| `STREAMING_RESPONSE` | `globals` | `HttpContextToken<boolean>` that enables stream handling for one request. |
| `STREAM_CONFIG` | `globals` | `HttpContextToken<StreamConfig>` containing one request's settings and handlers. |
| `streamSubscription<T>` | `globals` | Creates a typed observer for parsed stream payloads. |
| `ParsedData` | `globals` | Shape used by the package's built-in parsed-data model. |
| `StreamEventFunction<T>` | `globals` | Callback type receiving a parsed payload of type `T`. |
| `StreamConfig` | `globals` | Complete request-specific retry, timeout, and handler configuration. |
| `DelayNotifierType` | `globals` | Factory type for custom retry delay callbacks. |

## License

ISC
