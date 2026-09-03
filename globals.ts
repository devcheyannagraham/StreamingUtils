/**
 * Shared HttpContext tokens for flagging requests to interceptors, plus the
 * `streamSubscription` observer factory used to consume streamed, back-to-back
 * JSON responses.
 */
import {
  HttpContextToken,
  HttpEvent,
  HttpEventType,
  HttpErrorResponse,
} from "@angular/common/http";

import {
  Observable,
  RetryConfig,
  throwError,
  TimeoutConfig,
  timer,
} from "rxjs";

/** Marks a request as a long-lived streamed response so the interceptor applies retry and timeout handling. */
export const STREAMING_RESPONSE = new HttpContextToken<boolean>(() => false);

/** Supplies default retry, timeout, parsing, and error-handling behavior for each request. */
export const DEFAULT_STREAM_CONFIG: StreamConfig = {
  delay: 1_000,
  maxDelay: 5_000,
  jitter: 500,
  chunkTimeout: 7_000,
  maxRetryCount: 3,
  retryConfig: null,
  timeoutConfig: null,
  chunkTimeoutHandler: () =>
    throwError(() => new Error("Chunk Request timed out")),
  delayNotifier: delayNotifier,
  errorHandler: defaultErrorHandler,
  serverErrorCheck: serverErrorCheck,
};

export const STREAM_CONFIG = new HttpContextToken<Partial<StreamConfig>>(
  () => DEFAULT_STREAM_CONFIG,
);

/**
 * Builds an Observer for a streamed HTTP request and forwards each parsed
 * payload of type `T` to the caller's callbacks. The generic keeps the
 * callback payload aligned with the application's stream response shape.
 * `errorCB` receives the terminal error and `completeCB` runs when the stream ends.
 * @typeParam T Parsed payload type delivered to `nextCB`.
 */
export const streamSubscription = <T>({
  nextCB,
  errorCB,
  completeCB,
}: {
  nextCB?: StreamEventFunction<T>;
  errorCB?: (error?: Error | HttpErrorResponse) => any | void;
  completeCB?: Function;
} = {}): any => {
  return {
    // Forward parsed event data to the next callback.
    next: (event: HttpEvent<T>) => {
      // The interceptor adds parsedData to streamed HTTP events.
      // @ts-ignore
      const parsedData = event?.parsedData || {};
      if (nextCB) nextCB(parsedData, event);
    },
    error: (error?: Error | HttpErrorResponse) => {
      if (error instanceof HttpErrorResponse && error.status === 0) {
        console.error("Request timed out");
        errorCB?.(new Error("Request timed out: " + error.message));
        return;
      }

      errorCB?.(error);
    },
    complete: () => {
      if (completeCB) completeCB();
    },
  };
};

/** Creates the retry callback used by RxJS, with linear backoff and jitter from request settings. */
function delayNotifier({
  delay = DEFAULT_STREAM_CONFIG.delay,
  maxDelay = DEFAULT_STREAM_CONFIG.maxDelay,
  jitter = DEFAULT_STREAM_CONFIG.jitter,
  maxRetryCount = DEFAULT_STREAM_CONFIG.maxRetryCount,
}) {
  return (error?: Error, retryCount: number = maxRetryCount) => {
    const currentDelay =
      retryCount * delay + Math.floor(Math.random() * jitter);

    // Stop scheduling retries after the configured delay ceiling is exceeded.
    if (currentDelay > maxDelay) {
      return throwError(() => new Error("Maximum delay exceeded"));
    }

    console.warn(
      `${error?.message}:\nClient Retrying request after ${currentDelay}ms (retry count: ${retryCount})`,
    );
    return timer(currentDelay);
  };
}

/** Throws a parsed in-band server error so the interceptor's retry and error handlers can process it. */
function serverErrorCheck(event: HttpEvent<any>) {
  if (event.type === HttpEventType.DownloadProgress) {
    // @ts-ignore
    const error = event?.parsedData?.error;
    if (error) {
      throw new Error(error || "Unknown server error");
    }
  }
  return event;
}

/** Rethrows `error` once retries are exhausted, or immediately for non-streaming requests. */
function defaultErrorHandler(error: Error): Observable<never> {
  return throwError(() => error);
}

export type StreamEventFunction<T> = (
  parsedData?: T,
  event?: HttpEvent<T>,
) => any | void;

export type StreamConfig = {
  delay: number;
  maxDelay: number;
  jitter: number;
  chunkTimeout: number;
  maxRetryCount: number;
  retryConfig: RetryConfig | null;
  timeoutConfig?: number | Date | TimeoutConfig<HttpEvent<any>> | null;
  delayNotifier: DelayNotifierType;
  errorHandler: (error: Error) => Observable<Error>;
  serverErrorCheck: (event: HttpEvent<any>) => HttpEvent<Error>;
  chunkTimeoutHandler: (error: Error) => Observable<never>;
};

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
}) => (error?: Error, maxRetryCount?: number) => Observable<any>;
