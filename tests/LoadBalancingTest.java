import java.rmi.Naming;
import Clock.LamportResult;
import Clock.LogicalClock;

/**
 * Demonstrates the distinction between LOAD BALANCING (read-only calls,
 * round-robin across healthy instances) and LEADER-ONLY ROUTING (writes,
 * always sent to the current Bully-elected PRIMARY) through the Manager.
 *
 * This test issues N reads and N writes through the Manager only -- it
 * never talks to a backend instance directly, exactly like EVClient. Since
 * every replica holds identical replicated state, the response content is
 * the same regardless of which instance served it; the actual round-robin
 * distribution is visible in the Manager's own console output as
 * "[LOAD BALANCER] Selected <cluster>-<id> (read, round-robin)" lines --
 * run this test, then check the Manager's log for that rotation.
 */
public class LoadBalancingTest {

    private static final LogicalClock clock = new LogicalClock();

    public static void main(String[] args) throws Exception {
        String host = env("MANAGER_HOST", "localhost");
        String port = env("MANAGER_PORT", "1240");
        String base = "rmi://" + host + ":" + port + "/";

        System.out.println("============================================================");
        System.out.println("  LOAD BALANCING TEST (reads round-robin, writes leader-only)");
        System.out.println("============================================================");

        ChargingStationInterface station = (ChargingStationInterface) Naming.lookup(base + "ChargingStationService");
        PricingInterface pricing = (PricingInterface) Naming.lookup(base + "PricingService");

        int n = 6;
        System.out.println("\n[READ] Issuing " + n + " getStationStatus() calls (expect round-robin across CS1/CS2/CS3):");
        for (int i = 0; i < n; i++) {
            long sendL = clock.sendEvent();
            LamportResult<String> res = station.getStationStatus(sendL);
            clock.receiveEvent(res.getTimestamp());
            System.out.println("  call " + (i + 1) + ": " + res.getData());
        }

        System.out.println("\n[READ] Issuing " + n + " calculatePrice() calls (expect round-robin across P1/P2/P3):");
        for (int i = 0; i < n; i++) {
            long sendL = clock.sendEvent();
            LamportResult<Double> res = pricing.calculatePrice("S01", 1.0, sendL);
            clock.receiveEvent(res.getTimestamp());
            System.out.println("  call " + (i + 1) + ": price=" + res.getData());
        }

        System.out.println("\n[WRITE] Issuing " + n + " reserveAnyAvailablePort() calls "
                + "(expect ALL routed to the SAME current leader, never round-robined):");
        for (int i = 0; i < n; i++) {
            long sendL = clock.sendEvent();
            LamportResult<String> res = station.reserveAnyAvailablePort(sendL);
            clock.receiveEvent(res.getTimestamp());
            System.out.println("  call " + (i + 1) + ": allocated=" + res.getData());
            if (!"NONE".equals(res.getData())) {
                station.releasePort(res.getData(), clock.sendEvent());
            }
        }

        System.out.println("\n============================================================");
        System.out.println("Now check the Manager's console output for [LOAD BALANCER] lines:");
        System.out.println("  - read calls should show a rotating 'Selected ChargingStationService-N (read, round-robin)'");
        System.out.println("  - write calls should all show 'routing to leader ChargingStationService-N, bypassing round-robin'");
        System.out.println("============================================================");
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.trim().isEmpty()) ? v.trim() : def;
    }
}
