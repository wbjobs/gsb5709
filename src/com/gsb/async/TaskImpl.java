package com.gsb.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * State machine behind {@link Task}. Composition (thenApply/thenCombine/
 * allOf/anyOf) is callback driven, so no worker thread ever blocks waiting on
 * another task; worker threads only run user code and are interrupted on
 * cancellation, which keeps timed-out or cancelled pipelines from leaking
 * threads.
 */
final class TaskImpl<T> implements Task<T> {

    private static final int RUNNING = 0;
    private static final int SUCCESS = 1;
    private static final int FAILED = 2;
    private static final int CANCELLED = 3;

    /** Written before {@code result}/{@code error} reads become visible. */
    private volatile int state = RUNNING;
    private T result;
    private Throwable error;
    private Thread worker;
    private int dependents;
    private boolean detached;
    private final List<TaskImpl<?>> upstreams = new ArrayList<TaskImpl<?>>();
    private final List<Runnable> cancelCallbacks = new ArrayList<Runnable>();
    private final List<Runnable> completionListeners = new ArrayList<Runnable>();

    static <T> TaskImpl<T> start(Callable<T> work) {
        TaskImpl<T> task = new TaskImpl<T>();
        task.startWorker(work);
        return task;
    }

    // ---------------------------------------------------------------- get

    @Override
    public T get(long timeoutMillis) {
        boolean timedOut = false;
        synchronized (this) {
            if (timeoutMillis <= 0) {
                while (state == RUNNING) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AsyncException("interrupted while waiting for task", e);
                    }
                }
            } else {
                long deadline = System.currentTimeMillis() + timeoutMillis;
                while (state == RUNNING) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        timedOut = true;
                        break;
                    }
                    try {
                        wait(remaining);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new AsyncException("interrupted while waiting for task", e);
                    }
                }
            }
            if (!timedOut) {
                if (state == SUCCESS) {
                    return result;
                }
                if (state == FAILED) {
                    rethrow(error);
                }
                throw new AsyncCancellationException("task was cancelled");
            }
        }
        // Timed out: cancel so the worker thread is interrupted and cleaned up.
        cancel();
        throw new AsyncTimeoutException("task did not complete within " + timeoutMillis + " ms");
    }

    // ------------------------------------------------------------- cancel

    @Override
    public boolean cancel() {
        List<Runnable> listeners;
        List<Runnable> callbacks;
        Thread toInterrupt;
        synchronized (this) {
            if (state != RUNNING) {
                return false;
            }
            state = CANCELLED;
            notifyAll();
            listeners = drainListeners();
            callbacks = new ArrayList<Runnable>(cancelCallbacks);
            toInterrupt = worker;
        }
        runAll(listeners);
        runAll(callbacks);
        if (toInterrupt != null) {
            toInterrupt.interrupt();
        }
        detachUpstreams();
        return true;
    }

    @Override
    public boolean isDone() {
        return state != RUNNING;
    }

    @Override
    public Task<T> onCancel(Runnable callback) {
        boolean runNow;
        synchronized (this) {
            runNow = state == CANCELLED;
            if (state == RUNNING) {
                cancelCallbacks.add(callback);
            }
        }
        if (runNow) {
            callback.run();
        }
        return this;
    }

    // --------------------------------------------------------- composition

    @Override
    public <R> Task<R> thenApply(final Function<T, R> fn) {
        final TaskImpl<T> self = this;
        final TaskImpl<R> next = new TaskImpl<R>();
        next.upstreams.add(self);
        addDependent();
        whenComplete(new Runnable() {
            @Override
            public void run() {
                if (self.state == SUCCESS) {
                    final T value = self.result;
                    next.startWorker(new Callable<R>() {
                        @Override
                        public R call() {
                            return fn.apply(value);
                        }
                    });
                } else if (self.state == FAILED) {
                    next.failWith(self.error);
                } else {
                    next.cancel();
                }
            }
        });
        return next;
    }

    @Override
    public <U, R> Task<R> thenCombine(Task<U> other, final BiFunction<T, U, R> combiner) {
        final TaskImpl<T> self = this;
        final TaskImpl<U> that = cast(other);
        final TaskImpl<R> next = new TaskImpl<R>();
        next.upstreams.add(self);
        next.upstreams.add(that);
        addDependent();
        that.addDependent();
        final Object coord = new Object();
        final int[] completed = {0};
        final boolean[] dispatched = {false};
        Runnable step = new Runnable() {
            @Override
            public void run() {
                synchronized (coord) {
                    if (dispatched[0]) {
                        return;
                    }
                    if (self.state == FAILED) {
                        dispatched[0] = true;
                        next.failWith(self.error);
                        return;
                    }
                    if (that.state == FAILED) {
                        dispatched[0] = true;
                        next.failWith(that.error);
                        return;
                    }
                    if (self.state == CANCELLED || that.state == CANCELLED) {
                        dispatched[0] = true;
                        next.cancel();
                        return;
                    }
                    completed[0]++;
                    if (completed[0] == 2) {
                        dispatched[0] = true;
                        final T left = self.result;
                        final U right = that.result;
                        next.startWorker(new Callable<R>() {
                            @Override
                            public R call() {
                                return combiner.apply(left, right);
                            }
                        });
                    }
                }
            }
        };
        self.whenComplete(step);
        that.whenComplete(step);
        return next;
    }

    // ----------------------------------------------------- package-private

    void addUpstream(TaskImpl<?> upstream) {
        upstreams.add(upstream);
    }

    void addDependent() {
        synchronized (this) {
            dependents++;
        }
    }

    void whenComplete(Runnable listener) {
        boolean runNow;
        synchronized (this) {
            runNow = state != RUNNING;
            if (!runNow) {
                completionListeners.add(listener);
            }
        }
        if (runNow) {
            listener.run();
        }
    }

    /** Terminal transition for normal completion or failure of user code. */
    void complete(T value, Throwable err) {
        List<Runnable> listeners;
        synchronized (this) {
            if (state != RUNNING) {
                return;
            }
            if (err == null) {
                result = value;
                state = SUCCESS;
            } else {
                error = normalize(err);
                state = FAILED;
            }
            notifyAll();
            listeners = drainListeners();
        }
        runAll(listeners);
        detachUpstreams();
    }

    /** Fails this task with the exact same cause that failed an upstream. */
    void failWith(Throwable cause) {
        complete(null, cause);
    }

    boolean isRunning() {
        return state == RUNNING;
    }

    boolean isSuccess() {
        return state == SUCCESS;
    }

    boolean isFailed() {
        return state == FAILED;
    }

    T rawResult() {
        return result;
    }

    Throwable rawError() {
        return error;
    }

    // ----------------------------------------------------------- internals

    void startWorker(Callable<T> work) {
        final Callable<T> job = work;
        Thread thread;
        synchronized (this) {
            if (state != RUNNING) {
                return;
            }
            Async.WORKER_THREADS.incrementAndGet();
            thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        T value = null;
                        Throwable err = null;
                        try {
                            value = job.call();
                        } catch (Throwable t) {
                            err = t;
                        }
                        complete(value, err);
                    } finally {
                        Async.WORKER_THREADS.decrementAndGet();
                    }
                }
            });
            thread.setDaemon(true);
            thread.setName("gsb-async-worker-" + thread.getId());
            worker = thread;
        }
        thread.start();
    }

    /**
     * A downstream dependent went away. When the last dependent of a still
     * running task detaches, the task is no longer needed and is cancelled.
     */
    private void dependentCancelled() {
        synchronized (this) {
            if (dependents > 0) {
                dependents--;
            }
            if (dependents != 0 || state != RUNNING) {
                return;
            }
        }
        cancel();
    }

    private void detachUpstreams() {
        List<TaskImpl<?>> ups;
        synchronized (this) {
            if (detached) {
                return;
            }
            detached = true;
            ups = new ArrayList<TaskImpl<?>>(upstreams);
        }
        for (TaskImpl<?> up : ups) {
            up.dependentCancelled();
        }
    }

    private List<Runnable> drainListeners() {
        List<Runnable> copy = new ArrayList<Runnable>(completionListeners);
        completionListeners.clear();
        return copy;
    }

    private static void runAll(List<Runnable> callbacks) {
        for (Runnable r : callbacks) {
            try {
                r.run();
            } catch (Throwable ignored) {
                // Listener/callback failures must not break state propagation.
            }
        }
    }

    private static Throwable normalize(Throwable t) {
        if (t instanceof RuntimeException || t instanceof Error) {
            return t;
        }
        return new AsyncException(t);
    }

    private static void rethrow(Throwable t) {
        if (t instanceof RuntimeException) {
            throw (RuntimeException) t;
        }
        if (t instanceof Error) {
            throw (Error) t;
        }
        throw new AsyncException(t);
    }

    @SuppressWarnings("unchecked")
    static <U> TaskImpl<U> cast(Task<U> task) {
        if (!(task instanceof TaskImpl)) {
            throw new IllegalArgumentException("task was not created by com.gsb.async.Async");
        }
        return (TaskImpl<U>) task;
    }
}
