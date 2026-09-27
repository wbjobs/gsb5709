package com.gsb.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Entry point of the async task orchestrator.
 *
 * <p>Semantics guaranteed by this implementation:
 * <ul>
 *   <li>Cancelling a task propagates to its upstream dependencies; an upstream
 *       task is cancelled only when no other downstream is still waiting on it.</li>
 *   <li>A timeout in {@link Task#get(long)} cancels the task and interrupts its
 *       worker thread, so no thread is leaked.</li>
 *   <li>A failure propagates downstream as the very same {@link Throwable}
 *       instance (the root cause is never re-wrapped).</li>
 *   <li>{@link #allOf(List)} fails fast on the first failing element.</li>
 *   <li>{@link #anyOf(List)} completes with the first success and cancels the rest.</li>
 *   <li>Cancelling an already finished task returns {@code false} and does not
 *       change its result.</li>
 * </ul>
 */
public final class Async {

    public interface Task<T> {
        /**
         * Waits for the task to finish and returns its result.
         *
         * @param timeoutMillis maximum time to wait; {@code <= 0} means wait forever.
         * @return the task result.
         * @throws TaskTimeoutException  if the timeout elapses (the task is cancelled).
         * @throws CancellationException if the task was cancelled.
         * @throws RuntimeException      the original failure of the task, rethrown as-is.
         */
        T get(long timeoutMillis);

        /**
         * Cancels this task and propagates the cancellation upstream.
         *
         * @return {@code false} if the task was already finished (completed,
         *         failed or cancelled), {@code true} otherwise.
         */
        boolean cancel();

        /** @return {@code true} once the task reached a terminal state. */
        boolean isDone();

        /** Returns a task that applies {@code fn} to this task's result. */
        <R> Task<R> thenApply(Function<T, R> fn);

        /** Returns a task that combines this task's result with another task's result. */
        <U, R> Task<R> thenCombine(Task<U> other, BiFunction<T, U, R> fn);
    }

    private Async() {
    }

    /** Submits a body for asynchronous execution on its own worker thread. */
    public static <T> Task<T> submit(Callable<T> body) {
        return submit(body, null);
    }

    /**
     * Submits a body for asynchronous execution.
     *
     * @param onCancel callback invoked once if the task actually transitions to
     *                 the cancelled state (may be {@code null}).
     */
    public static <T> Task<T> submit(Callable<T> body, Runnable onCancel) {
        BasicTask<T> task = new BasicTask<T>();
        task.setBody(body);
        task.setOnCancel(onCancel);
        task.start();
        return task;
    }

    /**
     * Returns a task that completes with the results of all given tasks (in
     * order) once all of them complete, and fails fast with the root cause of
     * the first failing element without waiting for the others.
     */
    public static <T> Task<List<T>> allOf(List<? extends Task<T>> tasks) {
        final List<BasicTask<T>> elements = castAll(tasks);
        final BasicTask<List<T>> combined = new BasicTask<List<T>>();
        for (BasicTask<T> element : elements) {
            combined.addParent(element);
        }
        combined.setBody(new Callable<List<T>>() {
            @Override
            public List<T> call() {
                List<T> values = new ArrayList<T>(elements.size());
                for (BasicTask<T> element : elements) {
                    values.add(element.resultValue());
                }
                return values;
            }
        });
        for (BasicTask<T> element : elements) {
            element.addChild(combined);
            element.addListener(new BasicTask.Listener() {
                @Override
                public void onTerminal(BasicTask<?> task) {
                    combined.parentTerminated();
                }
            });
        }
        if (elements.isEmpty()) {
            combined.tryComplete(new ArrayList<T>(), null);
        }
        return combined;
    }

    /**
     * Returns a task that completes with the first successful element result;
     * all remaining elements are cancelled at that point. If every element
     * fails, the returned task fails with the last observed root cause.
     */
    public static <T> Task<T> anyOf(List<? extends Task<T>> tasks) {
        final List<BasicTask<T>> elements = castAll(tasks);
        if (elements.isEmpty()) {
            throw new IllegalArgumentException("anyOf requires at least one task");
        }
        final BasicTask<T> any = new BasicTask<T>();
        for (BasicTask<T> element : elements) {
            any.addParent(element);
        }
        final int[] failures = new int[1];
        final Throwable[] lastError = new Throwable[1];
        for (final BasicTask<T> element : elements) {
            element.addChild(any);
            element.addListener(new BasicTask.Listener() {
                @Override
                public void onTerminal(BasicTask<?> task) {
                    int state = element.stateSnapshot();
                    if (state == BasicTask.COMPLETED) {
                        if (any.tryComplete(element.resultValue(), null)) {
                            for (BasicTask<T> loser : elements) {
                                if (loser != element) {
                                    loser.cancel();
                                }
                            }
                        }
                    } else {
                        synchronized (failures) {
                            failures[0]++;
                            if (state == BasicTask.FAILED) {
                                lastError[0] = element.errorValue();
                            }
                            if (failures[0] == elements.size()) {
                                Throwable error = lastError[0];
                                if (error == null) {
                                    error = new CancellationException("all tasks were cancelled");
                                }
                                any.tryComplete(null, error);
                            }
                        }
                    }
                }
            });
        }
        return any;
    }

    @SuppressWarnings("unchecked")
    private static <T> List<BasicTask<T>> castAll(List<? extends Task<T>> tasks) {
        List<BasicTask<T>> out = new ArrayList<BasicTask<T>>(tasks.size());
        for (Task<T> task : tasks) {
            if (!(task instanceof BasicTask)) {
                throw new IllegalArgumentException("task was not created by Async: " + task);
            }
            out.add((BasicTask<T>) task);
        }
        return out;
    }
}
