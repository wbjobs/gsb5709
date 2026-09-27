package com.gsb.async;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * Default {@link Async.Task} implementation.
 *
 * <p>Lifecycle: {@code PENDING -> RUNNING -> COMPLETED | FAILED | CANCELLED}.
 * A task in {@code PENDING} waits for its parents; once every parent completed
 * successfully its body is started on a dedicated daemon worker thread. If any
 * parent fails, this task fails with the very same {@link Throwable} instance;
 * if any parent is cancelled, this task is cancelled as well.
 *
 * <p>Cancellation propagates upstream: every task tracks how many downstream
 * children are still waiting on it; when a child is cancelled and the count
 * drops to zero, the task cancels itself (interrupting its worker thread and
 * invoking its cancel callback), recursively.
 */
final class BasicTask<T> implements Async.Task<T> {

    static final String WORKER_PREFIX = "gsb-async-worker-";

    static final int PENDING = 0;
    static final int RUNNING = 1;
    static final int COMPLETED = 2;
    static final int FAILED = 3;
    static final int CANCELLED = 4;

    interface Listener {
        void onTerminal(BasicTask<?> task);
    }

    private static final AtomicInteger IDS = new AtomicInteger();

    private final Object lock = new Object();
    private final List<BasicTask<?>> parents = new ArrayList<BasicTask<?>>();
    private final List<Listener> listeners = new ArrayList<Listener>();

    private int state = PENDING;
    private T result;
    private Throwable error;
    private Thread worker;
    private Runnable onCancel;
    private Callable<T> body;
    private int waitingChildren;

    // ------------------------------------------------------------------ setup

    void setBody(Callable<T> body) {
        this.body = body;
    }

    void setOnCancel(Runnable onCancel) {
        this.onCancel = onCancel;
    }

    void addParent(BasicTask<?> parent) {
        parents.add(parent);
    }

    void addChild(BasicTask<?> child) {
        synchronized (lock) {
            waitingChildren++;
        }
    }

    void addListener(Listener listener) {
        boolean fireNow;
        synchronized (lock) {
            fireNow = isTerminal();
            if (!fireNow) {
                listeners.add(listener);
            }
        }
        if (fireNow) {
            listener.onTerminal(this);
        }
    }

    /** Starts a source task (one without parents). */
    void start() {
        runBody(body);
    }

    // ------------------------------------------------------------- state api

    int stateSnapshot() {
        synchronized (lock) {
            return state;
        }
    }

    T resultValue() {
        synchronized (lock) {
            return result;
        }
    }

    Throwable errorValue() {
        synchronized (lock) {
            return error;
        }
    }

    private boolean isTerminal() {
        return state == COMPLETED || state == FAILED || state == CANCELLED;
    }

    // ------------------------------------------------------------- execution

    /**
     * Called every time a parent reaches a terminal state. Fails fast with the
     * parent's root cause, cancels itself on parent cancellation, and starts
     * the body once all parents completed.
     */
    void parentTerminated() {
        synchronized (lock) {
            if (state != PENDING) {
                return;
            }
        }
        Throwable failure = null;
        boolean cancelled = false;
        boolean allCompleted = true;
        for (BasicTask<?> parent : parents) {
            int parentState = parent.stateSnapshot();
            if (parentState == FAILED) {
                failure = parent.errorValue();
                break;
            }
            if (parentState == CANCELLED) {
                cancelled = true;
                break;
            }
            if (parentState != COMPLETED) {
                allCompleted = false;
                break;
            }
        }
        if (failure != null) {
            tryComplete(null, failure);
        } else if (cancelled) {
            cancel();
        } else if (allCompleted) {
            runBody(body);
        }
    }

    private void runBody(final Callable<T> callable) {
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                T value = null;
                Throwable failure = null;
                try {
                    value = callable.call();
                } catch (Throwable t) {
                    failure = t;
                }
                tryComplete(value, failure);
            }
        }, WORKER_PREFIX + IDS.incrementAndGet());
        thread.setDaemon(true);
        synchronized (lock) {
            if (state != PENDING) {
                return;
            }
            state = RUNNING;
            worker = thread;
        }
        thread.start();
    }

    /**
     * Transitions to COMPLETED (failure == null) or FAILED, unless the task is
     * already in a terminal state.
     *
     * @return {@code true} if this call performed the transition.
     */
    boolean tryComplete(T value, Throwable failure) {
        List<Listener> toFire;
        List<BasicTask<?>> upstream;
        synchronized (lock) {
            if (isTerminal()) {
                return false;
            }
            if (failure == null) {
                state = COMPLETED;
                result = value;
            } else {
                state = FAILED;
                error = failure;
            }
            toFire = drainListeners();
            upstream = new ArrayList<BasicTask<?>>(parents);
            lock.notifyAll();
        }
        fireListeners(toFire);
        for (BasicTask<?> parent : upstream) {
            parent.childTerminated(false);
        }
        return true;
    }

    // ---------------------------------------------------------------- cancel

    @Override
    public boolean cancel() {
        List<Listener> toFire;
        List<BasicTask<?>> upstream;
        Thread workerThread;
        Runnable callback;
        synchronized (lock) {
            if (isTerminal()) {
                return false;
            }
            state = CANCELLED;
            toFire = drainListeners();
            upstream = new ArrayList<BasicTask<?>>(parents);
            workerThread = worker;
            callback = onCancel;
            lock.notifyAll();
        }
        if (workerThread != null) {
            workerThread.interrupt();
        }
        if (callback != null) {
            callback.run();
        }
        fireListeners(toFire);
        for (BasicTask<?> parent : upstream) {
            parent.childTerminated(true);
        }
        return true;
    }

    /**
     * Called by a child that reached a terminal state. When the child was
     * cancelled and no other child is still waiting on this task, the
     * cancellation propagates: this task cancels itself.
     */
    void childTerminated(boolean cancelled) {
        boolean cancelSelf = false;
        synchronized (lock) {
            waitingChildren--;
            if (cancelled && waitingChildren <= 0 && (state == PENDING || state == RUNNING)) {
                cancelSelf = true;
            }
        }
        if (cancelSelf) {
            cancel();
        }
    }

    // ------------------------------------------------------------------- get

    @Override
    public T get(long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        synchronized (lock) {
            while (state == PENDING || state == RUNNING) {
                if (timeoutMillis > 0) {
                    long remaining = deadline - System.currentTimeMillis();
                    if (remaining <= 0) {
                        break;
                    }
                    await(remaining);
                } else {
                    await(0);
                }
            }
            if (state == COMPLETED) {
                return result;
            }
            if (state == FAILED) {
                return rethrow(error);
            }
            if (state == CANCELLED) {
                throw new CancellationException("task was cancelled");
            }
        }
        // Timed out: cancel so the worker thread is interrupted and cleaned up.
        cancel();
        throw new TaskTimeoutException("task did not complete within " + timeoutMillis + " ms");
    }

    private void await(long millis) {
        try {
            lock.wait(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("interrupted while waiting for task", e);
        }
    }

    private static <T> T rethrow(Throwable failure) {
        if (failure instanceof RuntimeException) {
            throw (RuntimeException) failure;
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw new RuntimeException(failure);
    }

    @Override
    public boolean isDone() {
        synchronized (lock) {
            return isTerminal();
        }
    }

    // ------------------------------------------------------------ combinators

    @Override
    public <R> Async.Task<R> thenApply(final Function<T, R> fn) {
        final BasicTask<T> self = this;
        final BasicTask<R> child = new BasicTask<R>();
        child.addParent(self);
        child.setBody(new Callable<R>() {
            @Override
            public R call() {
                return fn.apply(self.resultValue());
            }
        });
        wire(self, child);
        return child;
    }

    @Override
    public <U, R> Async.Task<R> thenCombine(Async.Task<U> other, final BiFunction<T, U, R> fn) {
        final BasicTask<T> self = this;
        if (!(other instanceof BasicTask)) {
            throw new IllegalArgumentException("task was not created by Async: " + other);
        }
        final BasicTask<U> second = (BasicTask<U>) other;
        final BasicTask<R> child = new BasicTask<R>();
        child.addParent(self);
        child.addParent(second);
        child.setBody(new Callable<R>() {
            @Override
            public R call() {
                return fn.apply(self.resultValue(), second.resultValue());
            }
        });
        wire(self, child);
        wire(second, child);
        return child;
    }

    private static void wire(BasicTask<?> parent, final BasicTask<?> child) {
        parent.addChild(child);
        parent.addListener(new Listener() {
            @Override
            public void onTerminal(BasicTask<?> task) {
                child.parentTerminated();
            }
        });
    }

    // ---------------------------------------------------------------- helpers

    private List<Listener> drainListeners() {
        List<Listener> copy = new ArrayList<Listener>(listeners);
        listeners.clear();
        return copy;
    }

    private void fireListeners(List<Listener> toFire) {
        for (Listener listener : toFire) {
            listener.onTerminal(this);
        }
    }
}
