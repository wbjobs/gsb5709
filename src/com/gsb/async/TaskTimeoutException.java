package com.gsb.async;

/**
 * Thrown by {@link Async.Task#get(long)} when the task does not reach a
 * terminal state within the given timeout. The timed-out task is cancelled
 * (and its worker thread interrupted) before this exception is thrown, so a
 * timeout never leaks a worker thread.
 */
public class TaskTimeoutException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public TaskTimeoutException(String message) {
        super(message);
    }
}
