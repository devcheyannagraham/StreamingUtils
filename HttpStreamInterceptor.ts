/**
 * Functional HTTP interceptor that applies retry-on-failure and error handling to every
 * intercepted request, plus chunk timeout and NDJSON buffering for streamed requests
 * identified via the `STREAMING_RESPONSE` HttpContext token.
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
 * Intercepts HTTP requests and applies retry and error handling to all of them; requests marked with
 * `STREAMING_RESPONSE` additionally get chunk buffering and chunk-timeout handling for streamed NDJSON endpoints.
 * Why: Stream responses commit HTTP status 200 early in the header phase, requiring in-band error detection and stream chunk buffering over partial text deliveries.
 * @param req Angular HTTP request object to inspect and handle.
 * @param next Next interceptor or HTTP backend handler in the pipeline.
 * @returns Observable stream of HTTP events with retry/error handling applied, plus parsed JSON data chunks and timeout mechanics for streaming requests.
 */
export const HTTPStreamInterceptor: HttpInterceptorFn = (req, next) => {
  logDebug(
    "HTTPStreamInterceptor: Intercepting streaming request",
    req.urlWithParams,
  );
  // Merge request-scoped overrides over the library defaults.
  const config = {
    ...getDefaultConfig(),
    ...req.context.get(STREAM_CONFIG),
  };
  // Use custom retry settings when provided; otherwise build them from the active configuration.
  let retryConfig = config?.retryConfig || {
    count: config.maxRetryCount,
    delay: config.delayNotifier(config),
    resetOnSuccess: config?.resetOnSuccess,
  };
  // Use custom timeout settings when provided; otherwise use the chunk timeout configuration.
  let timeoutConfig = config?.timeoutConfig || {
    each: config.chunkTimeout,
    with: config.chunkTimeoutHandler,
  };

  // Build the timeout operator via explicit narrowing so TypeScript can resolve RxJS overloads.
  const timeoutOperator = (() => {
    // Keep number/date checks separate; combining them breaks overload narrowing for `timeout`.
    if (typeof timeoutConfig === "number") {
      // Timeout after N ms between emissions (and first emission when `first` is not provided).
      return timeout<HttpEvent<any>>(timeoutConfig);
    }

    if (timeoutConfig instanceof Date) {
      // Timeout at an absolute wall-clock time.
      return timeout<HttpEvent<any>>(timeoutConfig);
    }

    if (timeoutConfig.with) {
      // `with` switches to a fallback observable instead of emitting TimeoutError.
      return timeout({
        first: timeoutConfig.first,
        each: timeoutConfig.each,
        with: timeoutConfig.with,
        scheduler: timeoutConfig.scheduler,
        meta: timeoutConfig.meta,
      });
    }

    // Without `with`, timeout emits RxJS TimeoutError on expiry.
    return timeout<HttpEvent<any>>({
      first: timeoutConfig.first,
      each: timeoutConfig.each,
      scheduler: timeoutConfig.scheduler,
      meta: timeoutConfig.meta,
    });
  })();

  logDebug("HTTPStreamInterceptor: Active streaming configuration", {
    retryConfig,
    timeoutConfig,
    maxRetryCount: config.maxRetryCount,
  });

  const dataBuffer: dataBuffer = {
    partialTextLength: 0,
    buffer: "",
  };


  if (req.context.get(STREAMING_RESPONSE)) {
    return next(req).pipe(
      // Buffer streamed data, surface in-band errors, then apply timeout and retry handling.
      map((event) => bufferData(event, dataBuffer)),
      map((event) => config.serverErrorCheck(event)),
      timeoutOperator,
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
    return next(req).pipe(
      retry(retryConfig),
      catchError(config.errorHandler)
    );
  }
};

/**
 * Buffers streamed `DownloadProgress` chunks until complete newline-delimited JSON messages are formed, then attaches the parsed data to `event.parsedData`.
 * Why: Server packets can arrive across multiple progress events, so buffering prevents premature parsing of split JSON messages and resets when a request starts.
 * @param event Incoming HTTP event to process.
 * @param dbf Shared state object holding the accumulated buffer text and the last processed `partialText` length for the active stream.
 * @returns The HTTP event enriched with parsed payload data attached to `event.parsedData`.
 */
const bufferData = (event: HttpEvent<any>, dbf: dataBuffer) => {
  let stringChunks = [];
  let dataChunk: any;
  if (event.type === HttpEventType.DownloadProgress && event.partialText) {
    // Progress events repeat previously received text, so append only the new suffix.
    const newSlice = event.partialText.slice(dbf.partialTextLength);
    dbf.buffer += newSlice;
    logDebug("bufferData: Received DownloadProgress chunk slice", {
      newSliceLength: newSlice.length,
      totalPartialTextLength: event.partialText.length,
    });

    // Keep the final incomplete line buffered for the next progress event.
    stringChunks = dbf.buffer.split("\n");

    // handle incomplete data
    let lastStringChunk = stringChunks.pop() || "";
    if (lastStringChunk?.trim() == "") {
      dbf.buffer = "";
    } else {
      // assign remaining data to buffer for next chunk
      dbf.buffer = lastStringChunk;
      logDebug("bufferData: Incomplete chunk retained in buffer", dbf.buffer);
    }

    // Merge each complete JSON line into the parsed payload for this event.
    dataChunk = stringChunks.reduce((acc, chunk) => {
      let parsedChunk;

      try {
        parsedChunk = JSON.parse(chunk);
      } catch (error) {
        logDebug("bufferData: JSON parse error on chunk", { chunk, error });
        // Preserve malformed payloads as data rather than terminating the stream.
        return { ...acc, parseError: "Error parsing JSON chunk", chunk: chunk };
      }

      return { ...acc, ...parsedChunk };
    }, {});

    // Track the consumed text so the next progress event does not re-parse it.
    dbf.partialTextLength = event.partialText.length;
    logDebug("bufferData: Successfully processed chunks", {
      dataChunk,
      bufferRemaining: dbf.buffer,
    });
  } else if (event.type === HttpEventType.Sent) {
    // A new request starts a fresh stream buffer.
    dbf.partialTextLength = 0;
    dbf.buffer = "";
    logDebug(
      "bufferData: HttpEventType.Sent event received, reset buffer state",
    );
  }
  // @ts-ignore
  event["parsedData"] = dataChunk;
  return event;
};

type dataBuffer = {
  buffer: string;
  partialTextLength: number;
};
