/**
 * Functional HTTP interceptor that adds retry-on-failure, chunk timeout,
 * and JSON buffering handling for streamed NDJSON requests identified via
 * the `STREAMING_RESPONSE` HttpContext token.
 */
import {
  HttpEventType,
  HttpInterceptorFn,
  HttpEvent,
} from "@angular/common/http";
import {
  getDefaultConfig,
  STREAM_CONFIG,
  STREAMING_RESPONSE,
  logDebug,
} from "./globals.js";
import { timeout, retry, map, catchError } from "rxjs/operators";

/**
 * Intercepts HTTP requests and manages retry, timeout, buffering, and error handling for streamed NDJSON endpoints.
 * Why: Stream responses commit HTTP status 200 early in the header phase, requiring in-band error detection and stream chunk buffering over partial text deliveries.
 * @param req Angular HTTP request object to inspect and handle.
 * @param next Next interceptor or HTTP backend handler in the pipeline.
 * @returns Observable stream of HTTP events with parsed JSON data chunks and retry/timeout mechanics applied.
 */
export const HTTPStreamInterceptor: HttpInterceptorFn = (req, next) => {
  if (req.context.get(STREAMING_RESPONSE)) {
    logDebug(
      "HTTPStreamInterceptor: Intercepting streaming request",
      req.urlWithParams,
    );
    // Read this request's configuration, merging context token overrides over library defaults.
    const config = {
      ...getDefaultConfig(),
      ...req.context.get(STREAM_CONFIG),
    };
    // Use custom retryConfig if provided; otherwise assemble config from individual parameters.
    let retryConfig = config?.retryConfig || {
      count: config.maxRetryCount,
      delay: config.delayNotifier(config),
      resetOnSuccess: config?.resetOnSuccess,
    };
    // Use custom timeoutConfig if provided; otherwise construct default chunk timeout configuration.
    let timeoutConfig = config?.timeoutConfig || {
      each: config.chunkTimeout,
      with: config.chunkTimeoutHandler,
    };

    logDebug("HTTPStreamInterceptor: Active streaming configuration", {
      retryConfig,
      timeoutConfig,
      maxRetryCount: config.maxRetryCount,
    });

    let partialTextLength = 0;
    let buffer: string = "";

    return next(req).pipe(
      // Parse streamed data and surface any in-band server error to retry.
      map((event) => bufferData(event, buffer, partialTextLength)),
      map((event) => config.serverErrorCheck(event)),
      timeout<HttpEvent<any>>(timeoutConfig as any),
      retry(retryConfig),
      catchError(config.errorHandler),
    );
  }

  // Non-streaming requests.
  else {
    logDebug(
      "HTTPStreamInterceptor: Passing through non-streaming request",
      req.urlWithParams,
    );
    return next(req);
  }
};

/**
 * Buffers streamed `DownloadProgress` chunks until complete newline-delimited JSON messages are formed, then parses and attaches them to `event.parsedData`.
 * Why: Server data packets can arrive in fragments across multiple `DownloadProgress` events; buffering prevents premature JSON parsing errors on split chunks. Also resets buffer state on new request initiation (`HttpEventType.Sent`).
 * @param event Incoming HTTP event to process.
 * @param buffer Accumulated string buffer containing partial or unparsed chunk data.
 * @param partialTextLength Character index offset tracking previously read text from `event.partialText`.
 * @returns The HTTP event enriched with parsed payload data attached to `event.parsedData`.
 * @throws `Error` When an in-band server error property is encountered inside a parsed JSON chunk.
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
    const newSlice = event.partialText.slice(partialTextLength);
    buffer += newSlice;
    logDebug("bufferData: Received DownloadProgress chunk slice", {
      newSliceLength: newSlice.length,
      totalPartialTextLength: event.partialText.length,
    });

    // Extract complete, newline-delimited JSON messages from the buffer.
    stringChunks = buffer.split("\n");

    // handle incomplete data
    let lastStringChunk = stringChunks.pop() || "";
    if (lastStringChunk?.trim() == "") {
      buffer = "";
    } else {
      // assign remaining data to buffer for next chunk
      buffer = lastStringChunk;
      logDebug("bufferData: Incomplete chunk retained in buffer", buffer);
    }

    // convert string chunks to JSON objects
    // Merge all parsed chunks into a single object
    dataChunk = stringChunks.reduce((acc, chunk) => {
      let parsedChunk;

      try {
        parsedChunk = JSON.parse(chunk);
      } catch (error) {
        logDebug("bufferData: JSON parse error on chunk", { chunk, error });
        // this error is not retried
        return { ...acc, parseError: "Error parsing JSON chunk", chunk: chunk };
      }

      // detect in-band error marker and throw upstream if found, so retry logic can handle it.
      // only catch server errors. The server sends `error` as a plain string (see server.ts),
      // since JSON.stringify on an Error object drops message/stack (they're non-enumerable).
      if (parsedChunk?.error) {
        logDebug(
          "bufferData: In-band server error detected in chunk",
          parsedChunk.error,
        );
        throw new Error(parsedChunk.error || "Unknown server error");
      } else {
        return { ...acc, ...parsedChunk };
      }
    }, {});

    // Track last read position so the next chunk doesn't re-parse the same data.
    partialTextLength = event.partialText.length;
    logDebug("bufferData: Successfully processed chunks", {
      dataChunk,
      bufferRemaining: buffer,
    });
  } else if (event.type === HttpEventType.Sent) {
    // new request, reset partialTextLength
    partialTextLength = 0;
    buffer = "";
    logDebug(
      "bufferData: HttpEventType.Sent event received, reset buffer state",
    );
  }
  // @ts-ignore
  event["parsedData"] = dataChunk;
  return event;
};
