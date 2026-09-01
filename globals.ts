/**
 * Shared HttpContext tokens for flagging requests to interceptors, plus the
 * `streamSubscription` observer factory used to consume streamed, back-to-back
 * JSON responses.
 */
import { HttpContextToken, HttpEvent } from '@angular/common/http';

/** Marks a request as a long-lived streamed response so `retryInterceptor` applies retry/timeout handling to it. */
export const STREAMING_RESPONSE = new HttpContextToken<boolean>(() => false);

import { Observer } from 'rxjs';

/**
 * Builds the Observer for a streamed HTTP request: logs whatever value
 * arrives (already parsed by `retryInterceptor`) and forwards it to the
 * caller via callbacks, since an Observer's `next`/`error` return values are
 * discarded by RxJS and can't hand data back directly. `nextCB` gets each
 * emitted value, `errorCB` gets the raw error, `completeCB` fires when the
 * stream ends.
 */
export const streamSubscription = ({
  nextCB,
  errorCB,
  completeCB,
}: {
  nextCB?: StreamEventFunction;
  errorCB?: Function;
  completeCB?: Function;
} = {}): Partial<Observer<HttpEvent<string>>> => {
  return {
    // only forward data to the nextCB
    next: (event) => {
      // @ts-ignore
      //   extracts parsed data here to prevent @ts-ignore errors everywhere
      const parsedData = event?.parsedData || {};
      if (nextCB) nextCB(parsedData, event);
    },
    error: (error) => {
      if (error.status === 0) {
        console.error('POST stream request error: Request timed out');
      } else {
        console.error('POST stream request error:', error);
      }
      if (errorCB) errorCB(error);
    },
    complete: () => {
      console.log('POST stream request completed');
      if (completeCB) completeCB();
    },
  };
};

export type ParsedData = {
  retry?: any;
  error?: any;
  responseData?: any;
  done?: any;
};

export type StreamEventFunction = (parsedData?: ParsedData, event?: HttpEvent<any>) => any | void;
