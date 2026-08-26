package Clock;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe Lamport Logical Clock implementation for RMI distributed servers.
 * Implements standard Lamport clock rules:
 * - Local Event: L = L + 1
 * - Send Event: L = L + 1
 * - Receive Event: L = max(localClock, receivedTimestamp) + 1
 */
public class LogicalClock implements Serializable {
    private static final long serialVersionUID = 1L;
    private final AtomicLong clock;

    public LogicalClock() {
        this.clock = new AtomicLong(0);
    }

    public LogicalClock(long initialValue) {
        this.clock = new AtomicLong(initialValue);
    }

    /**
     * Returns current Lamport timestamp value without modifying it.
     */
    public long getValue() {
        return clock.get();
    }

    /**
     * Local event: L = L + 1
     */
    public long tick() {
        return clock.incrementAndGet();
    }

    /**
     * Send event: L = L + 1
     */
    public long sendEvent() {
        return clock.incrementAndGet();
    }

    /**
     * Receive event: L = max(localClock, receivedTimestamp) + 1
     * Atomic lock-free update loop.
     */
    public long receiveEvent(long receivedTimestamp) {
        while (true) {
            long current = clock.get();
            long updated = Math.max(current, receivedTimestamp) + 1;
            if (clock.compareAndSet(current, updated)) {
                return updated;
            }
        }
    }
}
