package com.gsb.async;

/** Thrown by {@link Task#get(long)} when the wait times out. */
public class AsyncTimeoutException extends AsyncException {
    public AsyncTimeoutException(String message) {
        super(message);
    }
}
