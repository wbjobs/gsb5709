package com.gsb.async;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Self-contained test harness (no JUnit): every test prints PASS/FAIL and the
 * process exits non-zero if anything failed.
 */
public final class AsyncTest {

    private static int passed;
    private static int failed;
    private static int baselineThreads;

    public static void main(String[] args) {
        baselineThreads = workerThreads();

        run("cancel propagates to exclusive upstream", new Case() {
            public void run() { testCancelPropagatesToExclusiveUpstream(); }
        });
        run("shared upstream is not cancelled while another downstream waits", new Case() {
            public void run() { testSharedUpstreamNotCancelledEarly(); }
        });
        run("timeout cleans up worker thread", new Case() {
            public void run() { testTimeoutCleansUpThread(); }
        });
        run("root cause survives two composition layers", new Case() {
            public void run() { testRootCausePreservedThroughTwoLayers(); }
        });
        run("allOf fails fast", new Case() {
            public void run() { testAllOfFailsFast(); }
        });
        run("anyOf returns first success and cancels the rest", new Case() {
            public void run() { testAnyOfFirstSuccess(); }
        });
        run("cancelling a completed task returns false", new Case() {
            public void run() { testCancelCompletedReturnsFalse(); }
        });
        run("thenCombine combines results", new Case() {
            public void run() { testThenCombine(); }
        });
        run("worker thread count returns to baseline", new Case() {
            public void run() { testThreadsBackToBaseline(); }
        });

        System.out.println();
        System.out.println("passed=" + passed + " failed=" + failed);
        if (failed > 0) {
            System.exit(1);
        }
    }

    // ------------------------------------------------------------------ tests

    /** Cancelling the only downstream must really cancel its upstream. */
    static void testCancelPropagatesToExclusiveUpstream() {
        final AtomicBoolean upstreamCancelled = new AtomicBoolean(false);
        Async.Task<String> upstream = Async.submit(sleeping(30000), new Runnable() {
            public void run() { upstreamCancelled.set(true); }
        });
        Async.Task<String> downstream = upstream.thenApply(new java.util.function.Function<String, String>() {
            public String apply(String s) { return s + "!"; }
        });

        assertTrue(downstream.cancel(), "cancel on pending downstream returns true");
        awaitTrue(upstreamCancelled, 5000, "exclusive upstream cancel callback invoked");
        assertTrue(upstream.isDone(), "upstream is done after cancellation propagated");
    }

    /** A shared upstream must survive until its last downstream is cancelled. */
    static void testSharedUpstreamNotCancelledEarly() {
        final AtomicBoolean upstreamCancelled = new AtomicBoolean(false);
        Async.Task<String> upstream = Async.submit(sleeping(30000), new Runnable() {
            public void run() { upstreamCancelled.set(true); }
        });
        java.util.function.Function<String, String> identity = new java.util.function.Function<String, String>() {
            public String apply(String s) { return s; }
        };
        Async.Task<String> childA = upstream.thenApply(identity);
        Async.Task<String> childB = upstream.thenApply(identity);

        assertTrue(childA.cancel(), "first child cancel returns true");
        sleep(200);
        assertFalse(upstreamCancelled.get(), "shared upstream NOT cancelled while childB waits");
        assertFalse(upstream.isDone(), "shared upstream still running while childB waits");

        assertTrue(childB.cancel(), "second child cancel returns true");
        awaitTrue(upstreamCancelled, 5000, "shared upstream cancelled once last downstream cancels");
    }

    /** A timeout must cancel the task and let its worker thread die. */
    static void testTimeoutCleansUpThread() {
        int before = workerThreads();
        Async.Task<String> slow = Async.submit(sleeping(30000));
        try {
            slow.get(100);
            fail("expected TaskTimeoutException");
        } catch (TaskTimeoutException expected) {
            // expected
        }
        assertTrue(slow.isDone(), "timed-out task is done (cancelled) after get timeout");
        awaitThreadCount(before, 5000);
    }

    /** The exact same exception instance must cross two composition layers. */
    static void testRootCausePreservedThroughTwoLayers() {
        final IllegalStateException root = new IllegalStateException("boom");
        Async.Task<String> upstream = Async.submit(new Callable<String>() {
            public String call() { throw root; }
        });
        Async.Task<String> mid = upstream.thenApply(new java.util.function.Function<String, String>() {
            public String apply(String s) { return s + "-mid"; }
        });
        Async.Task<String> down = mid.thenApply(new java.util.function.Function<String, String>() {
            public String apply(String s) { return s + "-down"; }
        });
        try {
            down.get(5000);
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertSame(root, e, "root cause instance unchanged after two layers");
        }
    }

    /** allOf must fail as soon as one element fails, without waiting. */
    static void testAllOfFailsFast() {
        final RuntimeException boom = new RuntimeException("fast-failure");
        Async.Task<String> bad = Async.submit(new Callable<String>() {
            public String call() throws Exception {
                Thread.sleep(100);
                throw boom;
            }
        });
        Async.Task<String> slow = Async.submit(sleeping(30000));
        Async.Task<List<String>> all = Async.allOf(Arrays.asList(bad, slow));

        long start = System.currentTimeMillis();
        try {
            all.get(20000);
            fail("expected allOf to fail");
        } catch (RuntimeException e) {
            long elapsed = System.currentTimeMillis() - start;
            assertSame(boom, e, "allOf fails with the element root cause");
            assertTrue(elapsed < 10000, "allOf failed fast, elapsed=" + elapsed + "ms");
        }
        assertTrue(all.isDone(), "allOf is done right after the first failure");
        assertFalse(slow.isDone(), "allOf does not cancel the still-running element");
        slow.cancel();
    }

    /** anyOf must return the first success and cancel the remaining tasks. */
    static void testAnyOfFirstSuccess() {
        final AtomicBoolean loserCancelled = new AtomicBoolean(false);
        Async.Task<String> winner = Async.submit(new Callable<String>() {
            public String call() throws Exception {
                Thread.sleep(100);
                return "win";
            }
        });
        Async.Task<String> loser = Async.submit(sleeping(30000), new Runnable() {
            public void run() { loserCancelled.set(true); }
        });
        Async.Task<String> any = Async.anyOf(Arrays.asList(winner, loser));

        assertEquals("win", any.get(10000), "anyOf returns the first success");
        awaitTrue(loserCancelled, 5000, "losing task is cancelled after first success");
    }

    /** Cancelling a completed task returns false and keeps the result. */
    static void testCancelCompletedReturnsFalse() {
        Async.Task<Integer> task = Async.submit(new Callable<Integer>() {
            public Integer call() { return 42; }
        });
        assertEquals(Integer.valueOf(42), task.get(5000), "task completes with 42");
        assertFalse(task.cancel(), "cancel on completed task returns false");
        assertEquals(Integer.valueOf(42), task.get(5000), "result unchanged after late cancel");
    }

    /** thenCombine waits for both parents and combines their results. */
    static void testThenCombine() {
        Async.Task<Integer> a = Async.submit(new Callable<Integer>() {
            public Integer call() { return 20; }
        });
        Async.Task<Integer> b = Async.submit(new Callable<Integer>() {
            public Integer call() { return 22; }
        });
        Async.Task<Integer> sum = a.thenCombine(b, new java.util.function.BiFunction<Integer, Integer, Integer>() {
            public Integer apply(Integer x, Integer y) { return x + y; }
        });
        assertEquals(Integer.valueOf(42), sum.get(5000), "thenCombine sums both results");
    }

    /** After the whole round, no worker thread may be left behind. */
    static void testThreadsBackToBaseline() {
        awaitThreadCount(baselineThreads, 10000);
    }

    // ---------------------------------------------------------------- helpers

    private interface Case {
        void run();
    }

    private static void run(String name, Case test) {
        try {
            test.run();
            passed++;
            System.out.println("PASS " + name);
        } catch (Throwable t) {
            failed++;
            System.out.println("FAIL " + name + " -> " + t);
        }
    }

    private static Callable<String> sleeping(final long millis) {
        return new Callable<String>() {
            public String call() throws Exception {
                Thread.sleep(millis);
                return "slept-" + millis;
            }
        };
    }

    private static int workerThreads() {
        int count = 0;
        for (Thread thread : Thread.getAllStackTraces().keySet()) {
            if (thread.isAlive() && thread.getName().startsWith(BasicTask.WORKER_PREFIX)) {
                count++;
            }
        }
        return count;
    }

    private static void awaitThreadCount(int expected, long timeoutMillis) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (workerThreads() <= expected) {
                return;
            }
            sleep(20);
        }
        fail("worker thread count did not return to " + expected + ", still " + workerThreads());
    }

    private static void awaitTrue(AtomicBoolean flag, long timeoutMillis, String message) {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            if (flag.get()) {
                return;
            }
            sleep(20);
        }
        fail("timed out waiting for: " + message);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void assertTrue(boolean condition, String message) {
        if (!condition) {
            fail("expected true: " + message);
        }
    }

    private static void assertFalse(boolean condition, String message) {
        if (condition) {
            fail("expected false: " + message);
        }
    }

    private static void assertEquals(Object expected, Object actual, String message) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            fail(message + " (expected=" + expected + ", actual=" + actual + ")");
        }
    }

    private static void assertSame(Object expected, Object actual, String message) {
        if (expected != actual) {
            fail(message + " (expected same instance)");
        }
    }

    private static void fail(String message) {
        throw new AssertionError(message);
    }
}
