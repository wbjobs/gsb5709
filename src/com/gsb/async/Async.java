package com.gsb.async;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Entry point of the mini async orchestrator.
 *
 * <ul>
 *   <li>{@link #submit(Callable)} runs work on a daemon worker thread.</li>
 *   <li>{@link #allOf(List)} fails fast: the first member failure completes
 *       the aggregate as failed with that exact cause and cancels the rest.</li>
 *   <li>{@link #anyOf(List)} returns the first success and cancels the rest.</li>
 * </ul>
 */
public final class Async {

    static final AtomicInteger WORKER_THREADS = new AtomicInteger(0);

    private Async() {
    }

    /** Submits {@code callable} for asynchronous execution. */
    public static <T> Task<T> submit(Callable<T> callable) {
        if (callable == null) {
            throw new NullPointerException("callable");
        }
        return TaskImpl.start(callable);
    }

    /**
     * Aggregates many tasks into one that succeeds with the list of results
     * (in input order) once every member succeeds. The first member failure
     * fails the aggregate immediately with the same root cause, without
     * waiting for the remaining members, which are then cancelled.
     */
    public static Task<List<Object>> allOf(List<? extends Task<?>> tasks) {
        if (tasks == null) {
            throw new NullPointerException("tasks");
        }
        final TaskImpl<List<Object>> aggregate = new TaskImpl<List<Object>>();
        final int size = tasks.size();
        if (size == 0) {
            aggregate.complete(new ArrayList<Object>(), null);
            return aggregate;
        }
        final List<TaskImpl<?>> members = new ArrayList<TaskImpl<?>>(size);
        for (Task<?> t : tasks) {
            members.add(TaskImpl.cast(t));
        }
        for (TaskImpl<?> member : members) {
            aggregate.addUpstream(member);
        }
        for (TaskImpl<?> member : members) {
            member.addDependent();
        }
        final Object[] results = new Object[size];
        final int[] succeeded = {0};
        final Object coord = new Object();
        for (int i = 0; i < size; i++) {
            final int index = i;
            final TaskImpl<?> member = members.get(i);
            member.whenComplete(new Runnable() {
                @Override
                public void run() {
                    synchronized (coord) {
                        if (!aggregate.isRunning()) {
                            return;
                        }
                        if (member.isFailed()) {
                            aggregate.failWith(member.rawError());
                            return;
                        }
                        if (!member.isSuccess()) {
                            aggregate.cancel();
                            return;
                        }
                        results[index] = member.rawResult();
                        succeeded[0]++;
                        if (succeeded[0] == size) {
                            aggregate.complete(
                                    new ArrayList<Object>(Arrays.asList(results)), null);
                        }
                    }
                }
            });
        }
        return aggregate;
    }

    /**
     * Returns a task that completes with the first successful member result.
     * The remaining members are cancelled. If every member fails, the
     * aggregate fails with the first member's root cause.
     */
    public static <T> Task<T> anyOf(List<? extends Task<T>> tasks) {
        if (tasks == null) {
            throw new NullPointerException("tasks");
        }
        if (tasks.isEmpty()) {
            throw new IllegalArgumentException("anyOf requires at least one task");
        }
        final TaskImpl<T> aggregate = new TaskImpl<T>();
        final List<TaskImpl<T>> members = new ArrayList<TaskImpl<T>>(tasks.size());
        for (Task<T> t : tasks) {
            members.add(TaskImpl.cast(t));
        }
        for (TaskImpl<T> member : members) {
            aggregate.addUpstream(member);
        }
        for (TaskImpl<T> member : members) {
            member.addDependent();
        }
        final int size = members.size();
        final int[] settled = {0};
        final Throwable[] firstError = {null};
        final Object coord = new Object();
        for (final TaskImpl<T> member : members) {
            member.whenComplete(new Runnable() {
                @Override
                public void run() {
                    synchronized (coord) {
                        if (!aggregate.isRunning()) {
                            return;
                        }
                        if (member.isSuccess()) {
                            aggregate.complete(member.rawResult(), null);
                            return;
                        }
                        settled[0]++;
                        if (member.isFailed() && firstError[0] == null) {
                            firstError[0] = member.rawError();
                        }
                        if (settled[0] == size) {
                            if (firstError[0] != null) {
                                aggregate.failWith(firstError[0]);
                            } else {
                                aggregate.cancel();
                            }
                        }
                    }
                }
            });
        }
        return aggregate;
    }

    /**
     * Number of worker threads currently alive. Returns to its starting value
     * once all tasks have completed, failed, or been cancelled and cleaned up.
     */
    public static int workerThreadCount() {
        return WORKER_THREADS.get();
    }
}
