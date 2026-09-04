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
}

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
 * Builds an Observer for a streamed HTTP request and forwards each parsed
 * payload of type `T` to the caller's callbacks. The generic keeps the
 * callback payload aligned with the application's stream response shape.
 * `errorCB` receives the terminal error and `completeCB` runs when the stream ends.
 * @typeParam T Parsed payload type delivered to `nextCB`.
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
    // Forward parsed event data to the next callback.
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

/**
 * Inspects `DownloadProgress` events for in-band error messages emitted by the backend and throws if found.
 * Why: Server streams return HTTP 200 before errors occur, so failures must be detected from chunk contents and thrown so RxJS retry and error pipelines trigger.
 * @param event Incoming HTTP event to inspect.
 * @returns The original `event` if no in-band error was present.
 * @throws `Error` When an in-band error marker exists on `event.parsedData`.
 */
function serverErrorCheck(event: HttpEvent<any>) {
  if (event.type === HttpEventType.DownloadProgress) {
    // @ts-ignore
    const error = event?.parsedData?.error;
    if (error) {
      logDebug("serverErrorCheck: In-band server error detected", error);
      throw new Error(error || "Unknown server error");
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
  errorHandler: (error:any, caught:Observable<any>) => ObservableInput<any>;
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
