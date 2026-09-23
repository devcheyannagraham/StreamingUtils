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
  TimeoutInfo,
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
 * Module-level default HTTP status codes treated as retryable by `delayNotifier`:
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

/** Holds module-level interceptor defaults; chunk buffering/timeout options apply only to streamed requests. */
let defaultStreamConfig: StreamConfig = {
  delay: 1_000,
  maxDelay: 5_000,
  jitter: 500,
  chunkTimeout: 7_000,
  maxRetryCount: 3,
  retryConfig: null,
  timeoutConfig: null,
  // Emit an HTTP-shaped timeout error so retry policy can evaluate `status` consistently.
  chunkTimeoutHandler: (_info) =>
    throwError(
      () =>
        new HttpErrorResponse({
          statusText: "Chunk Request timed out",
          status: 408,
        }),
    ),
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
 * `errorCB` receives terminal `HttpErrorResponse` values after retry handling, and `completeCB` runs when the stream ends.
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
  errorCB?: (error?: Partial<HttpErrorResponse>) => any | void;
  completeCB?: Function;
} = {}): any => {
  return {
    // Forward the full Angular event while also exposing the parsed NDJSON payload.
    next: (event: ExtendedHttpEvent) => {
      // The interceptor adds parsedData to streamed HTTP events.
      const parsedData = event?.parsedData || {};
      logDebug("streamSubscription: next event received", {
        event,
        parsedData,
      });
      if (nextCB) nextCB(parsedData, event);
    },
    error: (error?: Partial<HttpErrorResponse>) => {
      logDebug("streamSubscription: error received", error);
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
 * @returns A callback that throws immediately once `retryCount` exceeds `maxRetryCount` or for non-retryable errors; otherwise returns a timer Observable with the calculated delay capped at `maxDelay`.
 */
function delayNotifier({
  delay = defaultStreamConfig.delay,
  maxDelay = defaultStreamConfig.maxDelay,
  jitter = defaultStreamConfig.jitter,
  maxRetryCount = defaultStreamConfig.maxRetryCount,
  retryableErrorStatusCode = defaultStreamConfig.retryableErrorStatusCode,
}) {
  return (error: Partial<HttpErrorResponse>, retryCount: number = maxRetryCount) => {
    // Stop scheduling retries after max retries reached.
    if (retryCount > maxRetryCount) {
      logDebug("delayNotifier: Maximum retries reached, terminating retries");
      return throwError(() => error);
    }

    // Apply the configured status-code retry policy before calculating backoff.
    if (!retryableErrorStatusCode.includes(error?.status)) {
      logDebug(
        "delayNotifier: Non-Retryable Error:",
        error?.status,
        error?.message,
      );
      return throwError(() => (error));
    }

    // ensure delay doesn't exceed max
    const currentDelay = Math.min(
      retryCount * delay + Math.floor(Math.random() * jitter),
      maxDelay,
    );

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
 * @throws `HttpErrorResponse` constructed from the in-band error payload found in `event.parsedData`.
 */

function serverErrorCheck(event: ExtendedHttpEvent) {
  if (event.type === HttpEventType.DownloadProgress) {
    const error = event.parsedData?.error ? event.parsedData : false;

    if (error) {
      logDebug("serverErrorCheck detected error:", error);
      throw new HttpErrorResponse(error);
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
function defaultErrorHandler(error: Partial<HttpErrorResponse>): Observable<never> {
  logDebug("defaultErrorHandler: Handling terminal error", error);
  return throwError(() => error);
}

export type StreamEventFunction<T> = (
  parsedData?: T,
  event?: HttpEvent<T>,
) => any | void;

/**
 * Configures retry/error behavior for all intercepted requests and chunk-specific behavior for streamed requests.
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
  serverErrorCheck: (event: ExtendedHttpEvent) => HttpEvent<any>;
  chunkTimeoutHandler: (
    info: TimeoutInfo<HttpEvent<any>, unknown>,
  ) => Observable<never>;
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
}) => (error: Partial<HttpErrorResponse>, retryCount?: number) => Observable<any>;

export type ExtendedHttpEvent = HttpEvent<any> & {
  parsedData?: any;
};

