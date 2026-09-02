/**
 * Functional HTTP interceptor that adds retry-on-failure handling for
 * streamed requests (plain JSON objects), identified via
 * the `STREAMING_RESPONSE` context token set by `Requests.makeStreamRequest`
 * (../Services/requests.ts).
 */
import { throwError } from "rxjs";
import {
  HttpEventType,
  HttpInterceptorFn,
  HttpEvent,
} from "@angular/common/http";
import {
  DEFAULT_STREAM_CONFIG,
  STREAM_CONFIG,
  STREAMING_RESPONSE,
} from "./globals.js";
import { timeout, retry, map, catchError } from "rxjs/operators";

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
export const HTTPStreamInterceptor: HttpInterceptorFn = (req, next) => {
  // Read this request's configuration, including the context token's defaults.
  const config = {
    ...DEFAULT_STREAM_CONFIG,
    ...req.context.get(STREAM_CONFIG),
  };

  if (req.context.get(STREAMING_RESPONSE)) {
    let partialTextLength = 0;
    let buffer: string = "";

    return next(req).pipe(
      // Parse streamed data and surface any in-band server error to retry.
      map((event) => bufferData(event, buffer, partialTextLength)),
      map((event) => config.serverErrorCheck(event)),
      timeout({
        each: config.chunkTimeout,
        // The retry operator sends timeout errors to the configured notifier.
        with: () => {
          return throwError(() => new Error("Chunk Request timed out"));
        },
      }),
      retry({
        count: config.retryCount,
        delay: config.delayNotifier(config),
      }),
      catchError(config.errorHandler),
    );
  }

  // Non-streaming requests use the request's error handler without stream retries.
  else {
    return next(req).pipe(catchError(config.errorHandler));
  }
};

/**
 * Buffers a streamed `DownloadProgress` event's text until each newline-delimited
 * JSON message is complete, then parses and merges those messages onto `event.parsedData`.
 * Why: one event's `partialText` can end mid-message, or contain several messages,
 * so parsing per-event instead of per-message would throw on partial JSON or drop data.
 */
const bufferData = (
  event: HttpEvent<any>,
  buffer: string,
  partialTextLength: number,
) => {
  let stringChunks = [];
  let dataChunk: any;
  if (event.type === HttpEventType.DownloadProgress && event.partialText) {
    // only add new stuff to buffer
    buffer += event.partialText.slice(partialTextLength);

    // Extract complete, newline-delimited JSON messages from the buffer.
    stringChunks = buffer.split("\n");

    // handle incomplete data
    let lastStringChunk = stringChunks.pop() || "";
    if (lastStringChunk?.trim() == "") {
      buffer = "";
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
        return { ...acc, parseError: "Error parsing JSON chunk", chunk: chunk };
      }

      // detect in-band error marker and throw upstream if found, so retry logic can handle it.
      // only catch server errors. The server sends `error` as a plain string (see server.ts),
      // since JSON.stringify on an Error object drops message/stack (they're non-enumerable).
      if (parsedChunk?.error) {
        throw new Error(parsedChunk.error || "Unknown server error");
      } else {
        return { ...acc, ...parsedChunk };
      }
    }, {});

    // Track last read position so the next chunk doesn't re-parse the same data.
    partialTextLength = event.partialText.length;
  } else if (event.type === HttpEventType.Sent) {
    // new request, reset partialTextLength
    partialTextLength = 0;
    buffer = "";
  }
  // @ts-ignore
  event["parsedData"] = dataChunk;
  return event;
};
