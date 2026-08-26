package Clock;

/**
 * Standardized logger for RMI servers that outputs:
 * - Physical time
 * - Lamport logical timestamp
 * - Server name
 * - Thread ID and Thread Name
 * - Event type (SEND, RECEIVE, LOCAL)
 *
 * Uses strictly ASCII characters (e.g. -> instead of Unicode arrows).
 */
public class DistributedLogger {

    public static void log(String serverName, LogicalClock clock, String eventType, String message) {
        String physicalTime = PhysicalClock.getLocalPhysicalTimeFormatted();
        long lamport = (clock != null) ? clock.getValue() : 0;
        long threadId = Thread.currentThread().getId();
        String threadName = Thread.currentThread().getName();

        System.out.println("[Physical=" + physicalTime + "]");
        System.out.println("[Lamport=" + lamport + "]");
        System.out.println("[Server=" + serverName + "]");
        System.out.println("[Thread=" + threadId + " | " + threadName + "]");
        if (eventType != null && !eventType.trim().isEmpty()) {
            System.out.println("[Event=" + eventType.trim().toUpperCase() + "]");
        }
        System.out.println(message);
        System.out.println();
    }

    public static void log(String serverName, LogicalClock clock, String message) {
        log(serverName, clock, null, message);
    }
}
