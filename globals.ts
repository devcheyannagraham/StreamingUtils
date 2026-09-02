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

import { Observable, throwError, timer } from "rxjs";

/** Marks a request as a long-lived streamed response so the interceptor applies retry and timeout handling. */
export const STREAMING_RESPONSE = new HttpContextToken<boolean>(() => false);

const DEFAULT_DELAY = 1000; // base delay for linear backoff
const DEFAULT_MAX_DELAY = 15000; // maximum delay for linear backoff
const DEFAULT_JITTER = 500; // random jitter to avoid thundering herd problem
const DEFAULT_CHUNK_TIMEOUT = 5000;
const DEFAULT_REQUEST_TIMEOUT = 30000;
const DEFAULT_RETRY_COUNT = 3;

/** Supplies default retry, timeout, parsing, and error-handling behavior for each request. */
export const STREAM_CONFIG = new HttpContextToken<StreamConfig>(() => ({
  delay: DEFAULT_DELAY,
  maxDelay: DEFAULT_MAX_DELAY,
  jitter: DEFAULT_JITTER,
  chunkTimeout: DEFAULT_CHUNK_TIMEOUT,
  requestTimeout: DEFAULT_REQUEST_TIMEOUT,
  retryCount: DEFAULT_RETRY_COUNT,
  delayNotifier: delayNotifier,
  errorHandler: defaultErrorHandler,
  serverErrorCheck: serverErrorCheck,
}));

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
    // only forward data to the nextCB
    next: (event: HttpEvent<T>) => {
      //   extracts parsed data here to prevent @ts-ignore errors everywhere
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

/** Creates a retry callback with linear backoff and jitter from the request's delay settings. */
const delayNotifier: DelayNotifierType = ({
  delay = DEFAULT_DELAY,
  maxDelay = DEFAULT_MAX_DELAY,
  jitter = DEFAULT_JITTER,
}) => {
  return (error?: Error, retryCount: number = DEFAULT_RETRY_COUNT) => {
    const currentDelay =
      retryCount * delay + Math.floor(Math.random() * jitter);

    // Stop scheduling retries after the configured delay ceiling is exceeded.
    if (currentDelay > maxDelay) {
      return throwError(() => new Error("Maximum delay exceeded"));
    }

    console.warn(
      `${error}:\nClient Retrying request after ${currentDelay}ms (retry count: ${retryCount})`,
    );
    return timer(currentDelay);
  };
};

/** Throws the server's in-band error marker so the interceptor's retry and error handlers react to it. */
const serverErrorCheck = (event: HttpEvent<any>) => {
  if (event.type === HttpEventType.DownloadProgress) {
    // @ts-ignore
    const error = event?.parsedData?.error;
    if (error) {
      throw new Error(error || "Unknown server error");
    }
  }
  return event;
};

/** Rethrows `error` once retries are exhausted, or immediately for non-streaming requests. */
const defaultErrorHandler = (error: Error): Observable<never> => {
  return throwError(() => error);
};

export type StreamEventFunction<T> = (
  parsedData?: T,
  event?: HttpEvent<T>,
) => any | void;

export type StreamConfig = {
  delay: number;
  maxDelay: number;
  jitter: number;
  chunkTimeout: number;
  requestTimeout: number;
  retryCount: number;
  delayNotifier: DelayNotifierType;
  errorHandler: (error: Error) => Observable<never>;
  serverErrorCheck: (event: HttpEvent<any>) => HttpEvent<any>;
};

export type DelayNotifierType = ({
  delay,
  maxDelay,
  jitter,
}: {
  delay?: number;
  maxDelay?: number;
  jitter?: number;
}) => (error?: Error, retryCount?: number) => Observable<any>;
