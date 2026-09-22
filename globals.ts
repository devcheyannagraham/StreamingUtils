/**
 * Shared HttpContext tokens, streaming configuration, retry helpers, and the
 * `streamSubscription` observer factory used by the streaming interceptor.
 */
import {
  HttpContextToken,
  HttpEvent,
  HttpEventType,
  HttpErrorResponse,
} from "@angular/common/http";
import { signal } from "@angular/core";

import {
  Observable,
  RetryConfig,
  throwError,
  TimeoutConfig,
  timer,
  ObservableInput,
} from "rxjs";

/** Toggle flag that enables or disables debug logging across the streaming utilities. Defaults to false. */
const debug = signal(false);

/** Enables or disables diagnostic logging for the streaming utilities. */
export const setDebug = (value: boolean) => {
  debug.set(value);
};

/**
 * Conditionally logs debug messages prefixed with `HSI:` when debug mode is enabled.
 * Why: Centralizes logging output to avoid cluttered console output in production while aiding diagnosis during development.
 * @param args Arbitrary items or messages to log to the console.
 */
export const logDebug = (...args: any[]): void => {
  if (debug()) {
    console.log("HSI:", ...args);
  }
};

/** Marks a request as a long-lived streamed response so the interceptor applies retry and timeout handling. */
export const STREAMING_RESPONSE = new HttpContextToken<boolean>(() => false);

/**
 * Module-level default HTTP status codes treated as retryable by `retryableError`:
 * 408 Request Timeout, 429 Too Many Requests, 500 Internal Server Error,
 * 502 Bad Gateway, 503 Service Unavailable, and 504 Gateway Timeout.
 */
let DEFAULT_RETRYABLE_ERROR_STATUS_CODES: any[] = [
  0, 408, 429, 500, 502, 503, 504,
];

/**
 * Overrides the module-level default retryable HTTP status codes.
 * Why: Lets consumers customize which server error statuses trigger a retry app-wide.
 * @param codes Replacement list of HTTP status codes considered retryable.
 */
export const setDefaultRetryableErrorStatusCodes = (codes: any[]) => {
  DEFAULT_RETRYABLE_ERROR_STATUS_CODES = codes;
};

/**
 * Returns a shallow copy of the current default retryable HTTP status codes.
 * Why: Avoids exposing the internal array to direct external mutation.
 * @returns A clone of `DEFAULT_RETRYABLE_ERROR_STATUS_CODES`.
 */
export const getDefaultRetryableErrorCodes = () => {
  return [...DEFAULT_RETRYABLE_ERROR_STATUS_CODES];
};

/** Holds the module-level retry, timeout, buffering, and error-handling defaults for streaming requests. */
let defaultStreamConfig: StreamConfig = {
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
  retryableErrorStatusCode: DEFAULT_RETRYABLE_ERROR_STATUS_CODES,
};

/**
 * Updates the module-level default stream configuration by merging provided options into `defaultStreamConfig`.
 * Why: Allows consumers to set application-wide default streaming behavior once during app bootstrap.
 * @param config Partial stream configuration containing custom override values.
 */
export const setDefaultConfig = (config: Partial<StreamConfig>): void => {
  defaultStreamConfig = {
    ...defaultStreamConfig,
    ...config,
  };
};

/**
 * Returns a shallow copy of the current module-level default stream configuration.
 * Why: Allows consumers and interceptors to inspect active defaults without exposing the internal configuration object to direct external mutation.
 * @returns A clone of the current global `StreamConfig`.
 */
export const getDefaultConfig = (): StreamConfig => {
  return { ...defaultStreamConfig };
};

/** Request-scoped HttpContext token providing streaming configuration overrides. */
export const STREAM_CONFIG = new HttpContextToken<Partial<StreamConfig>>(
  () => defaultStreamConfig,
);

/**
 * Builds a convenience observer for streamed HTTP events.
 * The helper keeps the original Angular `HttpEvent` available to consumers while
 * also exposing the buffered NDJSON payload as `event.parsedData` for a typed,
 * app-friendly callback surface.
 * `errorCB` receives terminal errors after retry handling, and `completeCB` runs when the stream ends.
 * @typeParam T Parsed payload type used for `event.parsedData`.
 * @param callbacks Optional callbacks for next, error, and complete notifications.
 * @returns Observer object compatible with RxJS subscription methods.
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
    // Forward the full Angular event while also exposing the parsed NDJSON payload.
    next: (event: HttpEvent<T>) => {
      // The interceptor adds parsedData to streamed HTTP events.
      // @ts-ignore
      const parsedData = event?.parsedData || {};
      logDebug("streamSubscription: next event received", {
        event,
        parsedData,
      });
      if (nextCB) nextCB(parsedData, event);
    },
    error: (error?: Error | HttpErrorResponse) => {
      logDebug("streamSubscription: error received", error);
      if (error instanceof HttpErrorResponse && error.status === 0) {
        logDebug("streamSubscription: Request failed without an HTTP status");
        errorCB?.(new Error("Request failed: " + error.message));
        return;
      }

      errorCB?.(error);
    },
    complete: () => {
      logDebug("streamSubscription: stream completed");
      if (completeCB) completeCB();
    },
  };
};

/**
 * Creates the retry delay callback used by RxJS `retry`, applying linear backoff with jitter.
 * Why: Centralizes retry eligibility and timing for transport, timeout, and in-band errors.
 * @param config Delay, retry-limit, and retryable-status configuration.
 * @returns A callback that throws immediately for non-retryable errors or excessive delay; otherwise returns a timer Observable.
 */
function delayNotifier({
  delay = defaultStreamConfig.delay,
  maxDelay = defaultStreamConfig.maxDelay,
  jitter = defaultStreamConfig.jitter,
  maxRetryCount = defaultStreamConfig.maxRetryCount,
  retryableErrorStatusCode = defaultStreamConfig.retryableErrorStatusCode,
}) {
  return (error: ExtendedError, retryCount: number = maxRetryCount) => {
    // Apply the configured status-code retry policy before calculating backoff.
    if (!retryableErrorStatusCode.includes(error?.status)) {
      logDebug(
        "delayNotifier: Non-Retryable Error:",
        error.status,
        error.message,
      );
      return throwError(() => new Error(error.message));
    }

    const currentDelay =
      retryCount * delay + Math.floor(Math.random() * jitter);

    // Stop scheduling retries after the configured delay ceiling is exceeded.
    if (currentDelay > maxDelay) {
      logDebug("delayNotifier: Maximum delay exceeded, terminating retries");
      return throwError(() => new Error("Maximum delay exceeded"));
    }

    logDebug(
      `Retrying request after ${currentDelay}ms (retry count: ${retryCount}) due to: ${error?.message}`,
    );
    return timer(currentDelay);
  };
}

/**
 * Inspects `DownloadProgress` events for errors encoded in the streamed response body.
 * Why: Once a streaming response commits HTTP 200, backend failures must be converted into RxJS errors from the body.
 * @param event Incoming HTTP event to inspect.
 * @returns The original event when no in-band error is present.
 * @throws `HttpErrorResponse` when the in-band error includes a status; otherwise throws the original error object.
 */

function serverErrorCheck(event: ExtendedHttpEvent<any>) {
  if (event.type === HttpEventType.DownloadProgress) {
    const error = event.parsedData?.error ? event.parsedData : false;

    if (error) {
      logDebug("serverErrorCheck detected error:", error);
      throw error;
    }
  }
  return event;
}

/**
 * Default terminal error handler rethrowing errors for downstream consumers.
 * Why: Ensures unhandled errors properly terminate the stream and reach the subscriber's error callback.
 * @param error Error thrown during request processing or retry exhaustion.
 * @returns Observable that immediately errors with the provided error.
 */
function defaultErrorHandler(error: Error): Observable<never> {
  logDebug("defaultErrorHandler: Handling terminal error", error);
  return throwError(() => error);
}

export type StreamEventFunction<T> = (
  parsedData?: T,
  event?: HttpEvent<T>,
) => any | void;

/**
 * Configures buffering, timeout, retry, and error handling for intercepted streaming requests.
 * Why: Keeps request behavior customizable while sharing consistent defaults across the library.
 */
export type StreamConfig = {
  delay: number;
  maxDelay: number;
  jitter: number;
  chunkTimeout: number;
  maxRetryCount: number;
  resetOnSuccess?: boolean;
  retryConfig?: RetryConfig | null;
  timeoutConfig?: number | Date | TimeoutConfig<HttpEvent<any>> | null;
  delayNotifier: DelayNotifierType;
  errorHandler: (error: any, caught: Observable<any>) => ObservableInput<any>;
  serverErrorCheck: (event: HttpEvent<any>) => HttpEvent<any>;
  chunkTimeoutHandler: (error: Error) => Observable<never>;
  retryableErrorStatusCode: any[];
};

/** Factory type for retry delay callbacks supplied to RxJS `retry`. */
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
}) => (error: ExtendedError, retryCount?: number) => Observable<any>;

type ExtendedHttpEvent<T> = HttpEvent<T> & {
  parsedData?: T;
};

type ExtendedError =
  | HttpErrorResponse
  | {
      name: string;
      message: string;
      stack?: string;
      status: any;
    };
