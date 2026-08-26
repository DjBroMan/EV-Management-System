package Clock;

import java.rmi.Naming;

/**
 * Implements Cristian's Physical Clock Synchronization Algorithm.
 * Connects to TimeServer via RMI, measures RTT, estimates server physical time,
 * calculates local clock offset, and updates PhysicalClock.
 */
public class CristianClient {

    public static class SyncResult {
        public String serverName;
        public long t0Ms;
        public long t1Ms;
        public long serverTimeMs;
        public long rttMs;
        public long estimatedServerTimeMs;
        public long clockOffsetMs;
        public boolean success;
        public String errorMessage;
    }

    /**
     * Perform Cristian clock synchronization with reference TimeServer.
     */
    public static SyncResult synchronize(String serverName, String timeServerUrl) {
        SyncResult result = new SyncResult();
        result.serverName = serverName;

        try {
            TimeServerInterface timeServer = (TimeServerInterface) Naming.lookup(timeServerUrl);

            // 1. Record local time T0 before request
            long t0 = System.currentTimeMillis();

            // 2. Request physical time from TimeServer
            long serverTime = timeServer.getPhysicalTimeMillis();

            // 3. Record local time T1 after response
            long t1 = System.currentTimeMillis();

            // 4. Calculate RTT = T1 - T0
            long rtt = t1 - t0;

            // 5. Estimate TimeServer time = serverTime + (RTT / 2)
            long estimatedServerTime = serverTime + (rtt / 2);

            // 6. Calculate local clock offset = estimatedServerTime - T1
            long offset = estimatedServerTime - t1;

            // Update physical clock offset
            PhysicalClock.setClockOffsetMs(offset);

            result.t0Ms = t0;
            result.t1Ms = t1;
            result.serverTimeMs = serverTime;
            result.rttMs = rtt;
            result.estimatedServerTimeMs = estimatedServerTime;
            result.clockOffsetMs = offset;
            result.success = true;

            // Print formatted output as requested
            printSyncReport(result);

        } catch (Exception e) {
            result.success = false;
            result.errorMessage = e.getMessage();
            System.out.println("CRISTIAN CLOCK SYNCHRONIZATION FAILED for " + serverName + ": " + e.getMessage());
        }

        return result;
    }

    private static void printSyncReport(SyncResult res) {
        System.out.println("============================================================");
        System.out.println("CRISTIAN CLOCK SYNCHRONIZATION");
        System.out.println("============================================================");
        System.out.println("Server: " + res.serverName);
        System.out.println();
        System.out.println("Local time before synchronization:");
        System.out.println(PhysicalClock.formatTime(res.t0Ms));
        System.out.println();
        System.out.println("TimeServer time:");
        System.out.println(PhysicalClock.formatTime(res.serverTimeMs));
        System.out.println();
        System.out.println("Round-trip time:");
        System.out.println(res.rttMs + " ms");
        System.out.println();
        System.out.println("Estimated TimeServer time:");
        System.out.println(PhysicalClock.formatTime(res.estimatedServerTimeMs));
        System.out.println();
        System.out.println("Calculated clock offset:");
        System.out.println(res.clockOffsetMs + " ms");
        System.out.println();
        System.out.println("Synchronization completed.");
        System.out.println("============================================================");
    }
}
