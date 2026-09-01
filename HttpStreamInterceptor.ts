/**
 * Functional HTTP interceptor that adds retry-on-failure handling for
 * streamed requests (plain JSON objects), identified via
 * the `STREAMING_RESPONSE` context token set by `Requests.makeStreamRequest`
 * (../Services/requests.ts).
 */
import { throwError, timer } from 'rxjs';
import { HttpEventType, HttpHandlerFn, HttpRequest, HttpEvent } from '@angular/common/http';
import { STREAMING_RESPONSE } from './globals.js';
import { timeout, retry, map, catchError } from 'rxjs/operators';

/**
 * Detects the server's in-band error marker in a streamed response and
 * retries the request when it appears. Why: the response headers commit the
 * HTTP status to `200` before any error can occur, so the server can't
 * signal a mid-stream failure via status code — it has to write an in-band
 * marker instead, which isn't something the browser's normal error handling
 * reacts to on its own. Since one server write can straddle multiple
 * `DownloadProgress` chunks (or a chunk can contain several newline-delimited
 * JSON messages), this buffers partial text until each message is complete
 * before parsing it.
 */
export const HTTPStreamInterceptor = (req: HttpRequest<any>, next: HttpHandlerFn) => {
  if (req.context.get(STREAMING_RESPONSE)) {
    let partialTextLength = 0; // Track the length of the partial text received so far
    let buffer: string = '';

    return next(req).pipe(
      //  Intercept and parse data, throwing error upstream if the server signals an in-band error mid-stream.
      map((event) => bufferData(event, buffer, partialTextLength)),
      timeout({ each: 5000 }),
      retry({ count: 3, delay: delayNotifier() }),
      catchError(defaultErrorHandler),
    );
  }

  // Non-streaming requests just get the shared error logging, no retry/timeout handling.
  else {
    return next(req).pipe(catchError(defaultErrorHandler));
  }
};

/** Builds a retry delay function with a linear backoff plus jitter (see requests.ts for the same pattern). */
const delayNotifier = ({
  delay = 1000,
  maxDelay = 15000,
  jitter = 500,
}: {
  delay?: number;
  maxDelay?: number;
  jitter?: number;
} = {}) => {
  return (error: any, retryCount: any) => {
    // Guard clause: bail out of retrying once the configured delay ceiling is exceeded.
    if (delay > maxDelay) {
      return throwError(() => new Error('Maximum delay exceeded'));
    }
    const currentDelay = retryCount * delay + Math.floor(Math.random() * jitter);

    console.log(`Client Retrying request after ${currentDelay}ms (retry count: ${retryCount})`);
    return timer(currentDelay);
  };
};

const defaultErrorHandler = (error: any) => {
  return throwError(() => error);
};

const bufferData = (event: HttpEvent<any>, buffer: string, partialTextLength: number) => {
  let stringChunks = [];
  let dataChunk: any;
  if (event.type === HttpEventType.DownloadProgress && event.partialText) {
    // only add new stuff to buffer
    buffer += event.partialText.slice(partialTextLength);

    // Extract complete, newline-delimited JSON messages from the buffer.
    stringChunks = buffer.split('\n');

    // handle incomplete data
    let lastStringChunk = stringChunks.pop() || '';
    if (lastStringChunk?.trim() == '') {
      buffer = '';
    } else {
      // assign remaining data to buffer for next chunk
      buffer = lastStringChunk;
    }

    // convert string chunks to JSON objects
    // Merge all parsed chunks into a single object
    dataChunk = stringChunks.reduce((acc, chunk) => {
      let parsedChunk;

      try {
        parsedChunk = JSON.parse(chunk);
      } catch (error) {
        // this error is not retried
        return { ...acc, error: 'Error parsing JSON chunk', chunk: chunk };
      }

      // detect in-band error marker and throw upstream if found, so retry logic can handle it.
      // only catch server errors. The server sends `error` as a plain string (see server.ts),
      // since JSON.stringify on an Error object drops message/stack (they're non-enumerable).
      if (parsedChunk?.error) {
        console.error('server error', parsedChunk);
        throw new Error(parsedChunk.error || 'Unknown server error');
      } else {
        return { ...acc, ...parsedChunk };
      }
    }, {});

    // Update the length of the partial text received so far.
    // track last read position so we don't re-parse the same data in the next chunk
    partialTextLength = event.partialText.length;
  } else if (event.type === HttpEventType.Sent) {
    // new request, reset partialTextLength
    partialTextLength = 0;
    buffer = '';
  }
  // @ts-ignore
  event['parsedData'] = dataChunk;
  return event;
};
