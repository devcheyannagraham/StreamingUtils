/**
 * Shared HttpContext tokens for flagging requests to interceptors, default
 * stream configuration defaults, and the `streamSubscription` observer factory
 * used to consume streamed, newline-delimited JSON responses.
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
  catchError,
  ObservableInput,
} from "rxjs";

/** Toggle flag to enable or disable debug logging across the streaming utilities. Defaults to false. */
const debug = signal(false);

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

/** Supplies default retry, timeout, and error-handling configuration for streaming requests. */
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

/** Request-scoped HttpContext token providing customized stream configuration overrides. */
export const STREAM_CONFIG = new HttpContextToken<Partial<StreamConfig>>(
  () => defaultStreamConfig,
);

/**
 * Builds a convenience observer for streamed HTTP events.
 * The helper keeps the original Angular `HttpEvent` available to consumers while
 * also exposing the buffered NDJSON payload as `event.parsedData` for a typed,
 * app-friendly callback surface.
 * `errorCB` receives the terminal error and `completeCB` runs when the stream ends.
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
        logDebug("streamSubscription: Request timed out (status 0)");
        errorCB?.(new Error("Request timed out: " + error.message));
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
 * Creates the retry delay callback factory used by RxJS `retry`, applying linear backoff with random jitter.
 * Why: Spreads retry attempts over time to prevent hammering an overloaded or failing server.
 * @param config Timing and retry limit settings.
 * @returns Delay notifier function taking the error and current retry attempt count, returning a timer Observable or throwing on ceiling breach.
 */
function delayNotifier({
  delay = defaultStreamConfig.delay,
  maxDelay = defaultStreamConfig.maxDelay,
  jitter = defaultStreamConfig.jitter,
  maxRetryCount = defaultStreamConfig.maxRetryCount,
}) {
  return (error?: Error, retryCount: number = maxRetryCount) => {
    const currentDelay =
      retryCount * delay + Math.floor(Math.random() * jitter);

    logDebug("delayNotifier: Calculating retry delay", {
      retryCount,
      currentDelay,
      maxDelay,
      errorMessage: error?.message,
    });

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

function retryableError(status: number | undefined = undefined) {
  // Default retryable HTTP status codes:
  // 408 Request Timeout, 429 Too Many Requests, 500 Internal Server Error,
  // 502 Bad Gateway, 503 Service Unavailable, and 504 Gateway Timeout.
  const RETRYABLE_CODES = [408, 429, 500, 502, 503, 504];

  // If error is in band w/o a status e.g. {error: "custom-server error"}
  if (!status) return true;

  if (RETRYABLE_CODES.includes(status)) return true;
  // error contains non retryable server status code
  else return false;
}

/**
 * Inspects `DownloadProgress` events for in-band stream errors emitted by the backend.
 * Why: A streaming response commits HTTP 200 before the body is complete, so the server may
 * send an error payload inside the body and the interceptor must surface it to RxJS retry logic.
 * @param event Incoming HTTP event to inspect.
 * @returns The original `event` if no in-band error was present.
 * @throws `Error` When an in-band error marker exists on `event.parsedData`.
 */
function serverErrorCheck(event: ExtendedHttpEvent<any>) {
  if (event.type === HttpEventType.DownloadProgress) {
    const error: ExtendedError = event.parsedData.error ? event.parsedData : {};

    if (error.error) {
      if (retryableError(error?.status)) {
        logDebug("serverErrorCheck: In-band server error detected", error);
        throw new Error(error.error || "Unknown server error");
      } else logDebug("Non Retryable Error Code:", error.status);
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
}) => (error?: Error, retryCount?: number) => Observable<any>;

type ExtendedError = Error & {
  error: string;
  status?: number;
  message?: any;
};

type ExtendedHttpEvent<T> = HttpEvent<T> & {
  parsedData?: T;
};
