package com.gsb.async;

import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * A handle to an asynchronous computation.
 *
 * <p>Cancellation propagates upstream: cancelling a task detaches it from the
 * tasks it depends on, and an upstream task that has no remaining downstream
 * dependents is cancelled for real (its worker thread is interrupted and its
 * cancellation callbacks run). An upstream shared by several downstreams is
 * only cancelled once the last dependent goes away.</p>
 */
public interface Task<T> {

    /**
     * Waits for the result up to {@code timeoutMillis} milliseconds.
     * A value {@code <= 0} waits indefinitely.
     *
     * <p>On timeout the task is cancelled (so its worker thread is cleaned up)
     * and {@link AsyncTimeoutException} is thrown. A failure is rethrown as the
     * exact same exception instance that failed the upstream task; checked
     * exceptions are wrapped once, at the source, in {@link AsyncException}.</p>
     */
    T get(long timeoutMillis);

    /**
     * Cancels this task and propagates the cancellation upstream.
     *
     * @return {@code false} if the task had already completed (successfully,
     *         failed or cancelled); in that case the result is unchanged.
     */
    boolean cancel();

    /** @return true once the task has reached a terminal state. */
    boolean isDone();

    /** Returns a task that applies {@code fn} to this task's result. */
    <R> Task<R> thenApply(Function<T, R> fn);

    /** Returns a task that combines this task's result with {@code other}'s. */
    <U, R> Task<R> thenCombine(Task<U> other, BiFunction<T, U, R> combiner);

    /**
     * Registers a callback that runs if this task transitions to cancelled.
     * If the task is already cancelled the callback runs immediately.
     */
    Task<T> onCancel(Runnable callback);
}
