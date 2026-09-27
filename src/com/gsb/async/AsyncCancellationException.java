package com.gsb.async;

/** Thrown by {@link Task#get(long)} when the task was cancelled. */
public class AsyncCancellationException extends AsyncException {
    public AsyncCancellationException(String message) {
        super(message);
    }
}
