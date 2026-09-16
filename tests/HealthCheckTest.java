import java.rmi.Naming;
import Common.ClusterNodeInterface;
import Clock.LamportResult;
import Clock.LogicalClock;

/**
 * Repeatedly pings every instance of every cluster directly (bypassing the
 * Manager) and prints a heartbeat-style report showing server id, service,
 * role, alive/dead, physical time, and Lamport time for each -- the same
 * information the Manager's own internal health monitor uses to decide
 * which instances are eligible for load balancing and who the current
 * leader is. Run this in a loop (or repeatedly) while killing/restarting an
 * instance to see it drop out of and back into the healthy set.
 */
public class HealthCheckTest {

    private static final LogicalClock clock = new LogicalClock();

    public static void main(String[] args) throws Exception {
        System.out.println("============================================================");
        System.out.println("  HEALTH CHECK / HEARTBEAT TEST");
        System.out.println("============================================================");

        report("ChargingStation", "ChargingStationService",
                env("STATION_INSTANCES", "1:localhost:1234,2:localhost:1244,3:localhost:1254"));
        report("Reservation", "Reservation",
                env("RESERVATION_INSTANCES", "3:localhost:1235,1:localhost:1245,2:localhost:1255"));
        report("ChargingSession", "ChargingSessionService",
                env("SESSION_INSTANCES", "1:localhost:1236,2:localhost:1246,3:localhost:1256"));
        report("Pricing", "PricingService",
                env("PRICING_INSTANCES", "1:localhost:1238,2:localhost:1248,3:localhost:1258"));
        report("Payment", "PaymentService",
                env("PAYMENT_INSTANCES", "1:localhost:1237,2:localhost:1247,3:localhost:1257"));
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.trim().isEmpty()) ? v.trim() : def;
    }

    private static void report(String label, String bindPrefix, String instancesCsv) {
        System.out.println("\n--- " + label + " ---");
        for (String entry : instancesCsv.split(",")) {
            String[] parts = entry.trim().split(":");
            if (parts.length != 3) continue;
            int id = Integer.parseInt(parts[0]);
            String host = parts[1];
            int port = Integer.parseInt(parts[2]);
            String url = "rmi://" + host + ":" + port + "/" + bindPrefix + "-" + id;
            long physicalNow = System.currentTimeMillis();
            try {
                ClusterNodeInterface node = (ClusterNodeInterface) Naming.lookup(url);
                long sendL = clock.sendEvent();
                LamportResult<Boolean> pingRes = node.ping(sendL);
                clock.receiveEvent(pingRes.getTimestamp());
                long rL = clock.sendEvent();
                LamportResult<String> roleRes = node.getRole(rL);
                clock.receiveEvent(roleRes.getTimestamp());

                System.out.println("  [HEALTH] server=" + bindPrefix + "-" + id
                        + " service=" + label
                        + " role=" + roleRes.getData()
                        + " status=ALIVE"
                        + " physicalTime=" + physicalNow
                        + " lamport=" + roleRes.getTimestamp());
            } catch (Exception e) {
                System.out.println("  [HEALTH] server=" + bindPrefix + "-" + id
                        + " service=" + label
                        + " role=UNKNOWN"
                        + " status=DEAD"
                        + " physicalTime=" + physicalNow
                        + " error=" + e.getClass().getSimpleName());
            }
        }
    }
}
