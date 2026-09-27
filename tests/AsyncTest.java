import com.gsb.async.Async;
import com.gsb.async.AsyncTimeoutException;
import com.gsb.async.Task;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncTest {

    private static int passed = 0;

    public static void main(String[] args) {
        int threadsAtStart = Async.workerThreadCount();

        testCancelPropagatesToExclusiveUpstream();
        testSharedUpstreamSurvivesUntilLastDependentCancels();
        testTimeoutCleansUpWorkerThreads();
        testRootCauseSurvivesTwoCompositionLayers();
        testAllOfFailsFast();
        testAllOfAggregatesInOrder();
        testAnyOfReturnsFirstSuccessAndCancelsRest();
        testCancelOfCompletedTaskReturnsFalse();
        testThenCombineSuccessAndFailure();

        waitForThreadCount(threadsAtStart, "worker threads back to start after whole run");
        System.out.println("ALL " + passed + " TESTS PASSED");
    }

    // Cancelling a downstream must really cancel an upstream it exclusively
    // depends on: the upstream's cancel callback fires and its worker dies.
    static void testCancelPropagatesToExclusiveUpstream() {
        final AtomicBoolean upstreamCancelled = new AtomicBoolean(false);
        Task<String> upstream = Async.submit(sleeper(30000, "never"));
        upstream.onCancel(new Runnable() {
            @Override
            public void run() {
                upstreamCancelled.set(true);
            }
        });
        Task<String> downstream = upstream.thenApply(x -> x + "!");

        check(downstream.cancel(), "downstream cancel() returns true");
        waitFor(upstreamCancelled, "exclusive upstream cancel callback invoked");
        check(upstream.isDone(), "exclusive upstream is done after propagation");
        pass("cancel propagates to exclusive upstream");
    }

    // An upstream shared by two downstreams must NOT be cancelled when only
    // one of them cancels; it is cancelled once the last dependent cancels.
    static void testSharedUpstreamSurvivesUntilLastDependentCancels() {
        final AtomicBoolean upstreamCancelled = new AtomicBoolean(false);
        Task<Integer> upstream = Async.submit(sleeper(30000, 1));
        upstream.onCancel(new Runnable() {
            @Override
            public void run() {
                upstreamCancelled.set(true);
            }
        });
        Task<Integer> down1 = upstream.thenApply(x -> x + 1);
        Task<Integer> down2 = upstream.thenApply(x -> x + 2);

        check(down1.cancel(), "first downstream cancel() returns true");
        sleepQuietly(200);
        check(!upstreamCancelled.get(), "shared upstream not cancelled while still needed");
        check(!upstream.isDone(), "shared upstream still running while still needed");

        check(down2.cancel(), "second downstream cancel() returns true");
        waitFor(upstreamCancelled, "shared upstream cancelled after last dependent left");
        pass("shared upstream cancelled only after last dependent cancels");
    }

    // A get() that times out must cancel the task and let its worker thread
    // die, so the worker thread count returns to where it started.
    static void testTimeoutCleansUpWorkerThreads() {
        int before = Async.workerThreadCount();
        final AtomicBoolean cancelled = new AtomicBoolean(false);
        Task<String> slow = Async.submit(sleeper(60000, "too-late"));
        slow.onCancel(new Runnable() {
            @Override
            public void run() {
                cancelled.set(true);
            }
        });
        try {
            slow.get(100);
            fail("expected AsyncTimeoutException");
        } catch (AsyncTimeoutException expected) {
            // expected
        }
        check(cancelled.get(), "timed-out task is cancelled");
        waitForThreadCount(before, "worker threads cleaned up after timeout");
        pass("timeout cancels task and releases worker threads");
    }

    // The exact root cause instance must cross two composition layers
    // unchanged: same type, same instance, no wrapper.
    static void testRootCauseSurvivesTwoCompositionLayers() {
        final IllegalStateException boom = new IllegalStateException("boom");
        Task<Integer> upstream = Async.submit(new Callable<Integer>() {
            @Override
            public Integer call() {
                throw boom;
            }
        });
        Task<Integer> mid = upstream.thenApply(x -> x + 1);
        Task<Integer> down = mid.thenApply(x -> x * 2);
        try {
            down.get(5000);
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            check(e == boom, "root cause is the exact same instance, not wrapped");
        }
        pass("root cause propagates through two layers unchanged");
    }

    // allOf must fail as soon as any member fails, without waiting for the
    // slow members; the still-running members get cancelled.
    static void testAllOfFailsFast() {
        final AtomicBoolean slowCancelled = new AtomicBoolean(false);
        Task<String> slow = Async.submit(sleeper(30000, "slow"));
        slow.onCancel(new Runnable() {
            @Override
            public void run() {
                slowCancelled.set(true);
            }
        });
        Task<String> bad = Async.submit(new Callable<String>() {
            @Override
            public String call() {
                sleepQuietly(100);
                throw new IllegalArgumentException("bad-member");
            }
        });
        Task<List<Object>> all = Async.allOf(Arrays.<Task<?>>asList(slow, bad));
        long started = System.currentTimeMillis();
        try {
            all.get(20000);
            fail("expected allOf to fail");
        } catch (IllegalArgumentException e) {
            check("bad-member".equals(e.getMessage()), "allOf fails with member root cause");
        }
        long elapsed = System.currentTimeMillis() - started;
        check(elapsed < 10000, "allOf failed fast without waiting for slow member (took "
                + elapsed + " ms)");
        waitFor(slowCancelled, "slow member cancelled after allOf failure");
        pass("allOf fails fast and cancels remaining members");
    }

    static void testAllOfAggregatesInOrder() {
        Task<String> a = Async.submit(sleeper(150, "a"));
        Task<String> b = Async.submit(sleeper(50, "b"));
        Task<List<Object>> all = Async.allOf(Arrays.<Task<?>>asList(a, b));
        List<Object> results = all.get(5000);
        check(results.size() == 2
                && "a".equals(results.get(0))
                && "b".equals(results.get(1)), "allOf keeps input order");
        pass("allOf aggregates results in input order");
    }

    // anyOf returns the first success and cancels the losers.
    static void testAnyOfReturnsFirstSuccessAndCancelsRest() {
        final AtomicBoolean slowCancelled = new AtomicBoolean(false);
        Task<String> slow = Async.submit(sleeper(30000, "slow"));
        slow.onCancel(new Runnable() {
            @Override
            public void run() {
                slowCancelled.set(true);
            }
        });
        Task<String> fast = Async.submit(sleeper(100, "fast"));
        Task<String> any = Async.anyOf(Arrays.asList(slow, fast));
        check("fast".equals(any.get(10000)), "anyOf returns first success");
        waitFor(slowCancelled, "losing member cancelled after anyOf settled");
        pass("anyOf returns first success and cancels the rest");
    }

    // Cancelling an already completed task returns false and keeps the result.
    static void testCancelOfCompletedTaskReturnsFalse() {
        Task<Integer> done = Async.submit(new Callable<Integer>() {
            @Override
            public Integer call() {
                return 42;
            }
        });
        check(done.get(5000) == 42, "task completed with 42");
        check(!done.cancel(), "cancel() on completed task returns false");
        check(done.isDone(), "completed task isDone");
        check(done.get(1000) == 42, "result unchanged after failed cancel");
        pass("cancel of completed task returns false and keeps result");
    }

    static void testThenCombineSuccessAndFailure() {
        Task<Integer> a = Async.submit(sleeper(50, 20));
        Task<Integer> b = Async.submit(sleeper(100, 22));
        check(a.thenCombine(b, (x, y) -> x + y).get(5000) == 42, "thenCombine sums results");

        final IllegalStateException boom = new IllegalStateException("combine-boom");
        Task<Integer> bad = Async.submit(new Callable<Integer>() {
            @Override
            public Integer call() {
                throw boom;
            }
        });
        Task<Integer> good = Async.submit(sleeper(50, 1));
        Task<Integer> combined = bad.thenCombine(good, (x, y) -> x + y);
        try {
            combined.get(5000);
            fail("expected IllegalStateException from thenCombine");
        } catch (IllegalStateException e) {
            check(e == boom, "thenCombine propagates root cause unchanged");
        }
        pass("thenCombine combines results and propagates root cause");
    }

    // ------------------------------------------------------------ helpers

    static <T> Callable<T> sleeper(final long millis, final T value) {
        return new Callable<T>() {
            @Override
            public T call() {
                sleepQuietly(millis);
                return value;
            }
        };
    }

    static void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    static void waitFor(AtomicBoolean flag, String message) {
        long deadline = System.currentTimeMillis() + 10000;
        while (!flag.get() && System.currentTimeMillis() < deadline) {
            sleepQuietly(20);
        }
        check(flag.get(), message);
    }

    static void waitForThreadCount(int expected, String message) {
        long deadline = System.currentTimeMillis() + 10000;
        // Threads from earlier tests may still be winding down, so "cleaned
        // up" means the count settles at or below the expected baseline.
        while (Async.workerThreadCount() > expected && System.currentTimeMillis() < deadline) {
            sleepQuietly(20);
        }
        check(Async.workerThreadCount() <= expected,
                message + " (expected " + expected + ", got " + Async.workerThreadCount() + ")");
    }

    static void check(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError("CHECK FAILED: " + message);
        }
    }

    static void fail(String message) {
        throw new AssertionError("CHECK FAILED: " + message);
    }

    static void pass(String name) {
        passed++;
        System.out.println("PASS: " + name);
    }
}
