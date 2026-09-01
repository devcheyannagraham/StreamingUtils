# StreamingUtils

Helpers for consuming server-sent [NDJSON](http://ndjson.org/) (newline-delimited JSON) streams from POST requests in an Angular app, built on Angular's `HttpClient` and RxJS.

Servers can't change the HTTP status code mid-response (headers commit to `200` before the body streams), so this package expects the server to write its body as NDJSON — plain JSON objects separated by `\n` — and to signal failures with an in-band `{ "error": "..." }` message. `HTTPStreamInterceptor` is an Angular `HttpInterceptorFn` that buffers and parses those chunks, detects the error marker, and retries the request with backoff; `streamSubscription` gives you a simple callback-based `Observer` to consume the parsed data from Angular's `HttpClient`.

## Installation

```bash
npm install @chey.dev/streamingutils
```

### Peer dependencies

- `@angular/common` (for Angular's `HttpClient`, `HttpContextToken`, `HttpEventType`)
- `rxjs`

## Response format expected by the client

The server must write the response body as [NDJSON](http://ndjson.org/): one JSON object per line, each terminated by a single `\n` (newline) character, e.g.:

```text
{"responseData":{"data1":"my custom data"}}\n{"done":true}\n
```

Each `\n`-terminated line must be valid, standalone JSON — not a JSON array, and not pretty-printed across multiple lines. `HTTPStreamInterceptor` buffers incoming bytes, splits on `\n`, and `JSON.parse`s each line; a line that isn't valid JSON or isn't newline-terminated will not be parsed correctly. Failures are signaled in-band with `{ "error": "..." }` (not via HTTP status, since the status is already committed to `200` once the body starts streaming).

## Usage

### 1. Register the interceptor

```ts
import { provideHttpClient, withInterceptors } from '@angular/common/http';
import { HTTPStreamInterceptor } from '@chey.dev/streamingutils/HttpStreamInterceptor';

export const appConfig = {
  providers: [provideHttpClient(withInterceptors([HTTPStreamInterceptor]))],
};
```

### 2. Mark a request as streamed

Set the `STREAMING_RESPONSE` context token on any request that should get retry/timeout handling and in-band error detection:

```ts
import { HttpContext } from '@angular/common/http';
import { STREAMING_RESPONSE } from '@chey.dev/streamingutils/globals';

this.http.post('/api/stream', body, {
  context: new HttpContext().set(STREAMING_RESPONSE, true),
  observe: 'events',
  reportProgress: true,
}).subscribe(streamSubscription({
  nextCB: (parsedData) => console.log('chunk', parsedData),
  errorCB: (error) => console.error('stream failed', error),
  completeCB: () => console.log('stream done'),
}));
```

Requests must be made with `observe: 'events'` and `reportProgress: true` so `DownloadProgress` events are emitted for the interceptor to buffer and parse.

### 3. Consume parsed events with `streamSubscription`

`streamSubscription` builds an RxJS `Observer` and forwards each parsed JSON chunk to your callbacks:

```ts
import { streamSubscription } from '@chey.dev/streamingutils/globals';

const observer = streamSubscription({
  nextCB: (parsedData, event) => { /* handle each parsed chunk */ },
  errorCB: (error) => { /* handle terminal error */ },
  completeCB: () => { /* stream finished */ },
});
```

### 4. Full example: Angular service + Node/Express server

A minimal Angular service that issues the streamed request and hands the response to `streamSubscription`:

```ts
import { Injectable } from '@angular/core';
import { HttpClient, HttpContext } from '@angular/common/http';
import { STREAMING_RESPONSE, streamSubscription } from '@chey.dev/streamingutils/globals';

@Injectable({ providedIn: 'root' })
export class StreamService {
  constructor(private http: HttpClient) {}

  makeStreamRequest() {
    this.http.post('/api/stream', {}, {
      context: new HttpContext().set(STREAMING_RESPONSE, true),
      observe: 'events',
      reportProgress: true,
      responseType: 'text',
    }).subscribe(streamSubscription({
      nextCB: (parsedData) => {
        if (parsedData?.responseData) console.log('data', parsedData.responseData);
        if (parsedData?.done) console.log('stream complete');
      },
      errorCB: (error) => console.error('stream failed', error),
    }));
  }
}
```

A matching Express handler that writes newline-delimited JSON chunks:

```ts
app.post('/api/stream', (req, res) => {
  res.write(JSON.stringify({ responseData: { data1: 'my custom data' } }) + '\n');
  res.write(JSON.stringify({ done: true }) + '\n');
  res.end();
});
```

## API

| Export | From | Description |
| --- | --- | --- |
| `HTTPStreamInterceptor` | `HttpStreamInterceptor` | Functional `HttpInterceptorFn` that buffers streamed chunks, retries on in-band errors, and applies a per-chunk timeout. |
| `STREAMING_RESPONSE` | `globals` | `HttpContextToken<boolean>` used to flag a request for streaming handling. |
| `streamSubscription` | `globals` | Builds an RxJS `Observer` with `nextCB`/`errorCB`/`completeCB` callbacks. |
| `ParsedData` | `globals` | Type describing a parsed stream chunk (`retry`, `error`, `responseData`, `done`). |
| `StreamEventFunction` | `globals` | Type for the `nextCB` callback signature. |

## License

ISC
