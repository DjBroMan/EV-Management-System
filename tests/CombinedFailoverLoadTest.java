import java.rmi.Naming;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import Clock.LogicalClock;

/**
 * Combined load-balancing + failover test: fires a steady stream of
 * concurrent reservation requests through the Manager for a fixed duration.
 * Intended to be started, then have a leader instance killed mid-run (see
 * docs/MANUAL_DEMONSTRATION.md) -- Bully election + Manager re-routing
 * should mean only a small, bounded number of requests fail around the
 * moment of the kill, with normal throughput resuming once a new leader is
 * announced and every subsequent request continues to succeed.
 */
public class CombinedFailoverLoadTest {

    private static final int DURATION_SECONDS = 30;
    private static final int THREADS = 5;

    public static void main(String[] args) throws Exception {
        String host = env("MANAGER_HOST", "localhost");
        String port = env("MANAGER_PORT", "1240");
        String url = "rmi://" + host + ":" + port + "/ReservationService";

        System.out.println("============================================================");
        System.out.println("  COMBINED LOAD BALANCING + FAILOVER TEST");
        System.out.println("  Running for " + DURATION_SECONDS + "s with " + THREADS
                + " concurrent client threads hitting the Manager.");
        System.out.println("  Kill the current Reservation leader now to observe failover.");
        System.out.println("============================================================");

        AtomicInteger success = new AtomicInteger(0);
        AtomicInteger failure = new AtomicInteger(0);
        long deadline = System.currentTimeMillis() + DURATION_SECONDS * 1000L;

        ExecutorService pool = Executors.newFixedThreadPool(THREADS);
        CountDownLatch done = new CountDownLatch(THREADS);

        for (int t = 0; t < THREADS; t++) {
            final int threadId = t;
            pool.submit(() -> {
                LogicalClock clock = new LogicalClock();
                int i = 0;
                while (System.currentTimeMillis() < deadline) {
                    try {
                        ReservationInterface reservation = (ReservationInterface) Naming.lookup(url);
                        String result = reservation.reserveSlot("load-thread-" + threadId, "vehicle-" + threadId + "-" + (i++));
                        if (result != null && result.contains("Reservation successful")) {
                            success.incrementAndGet();
                        } else {
                            failure.incrementAndGet();
                            System.out.println("  [thread-" + threadId + "] non-success response: " + result.replace("\n", " | "));
                        }
                    } catch (Exception e) {
                        failure.incrementAndGet();
                        System.out.println("  [thread-" + threadId + "] request FAILED (expected briefly during failover): "
                                + e.getClass().getSimpleName());
                    }
                    try { Thread.sleep(300); } catch (InterruptedException ignored) { }
                }
                done.countDown();
            });
        }

        done.await(DURATION_SECONDS + 15, TimeUnit.SECONDS);
        pool.shutdownNow();

        System.out.println("============================================================");
        System.out.println("RESULT: " + success.get() + " succeeded, " + failure.get() + " failed out of "
                + (success.get() + failure.get()) + " total requests.");
        System.out.println("A small failure count clustered around the kill, followed by 100% success "
                + "after the new leader is announced, demonstrates working failover under load.");
        System.out.println("============================================================");
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.trim().isEmpty()) ? v.trim() : def;
    }
}
