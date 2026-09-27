package com.gsb.async;

/**
 * Wraps a checked exception thrown by user code. The wrap happens exactly
 * once, at the task where the exception was raised; downstream tasks rethrow
 * this same instance so the root cause is never re-wrapped.
 */
public class AsyncException extends RuntimeException {
    public AsyncException(String message) {
        super(message);
    }

    public AsyncException(Throwable cause) {
        super(cause);
    }

    public AsyncException(String message, Throwable cause) {
        super(message, cause);
    }
}
