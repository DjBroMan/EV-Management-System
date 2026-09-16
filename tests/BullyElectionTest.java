import java.rmi.Naming;
import Common.ClusterNodeInterface;
import Clock.LamportResult;
import Clock.LogicalClock;

/**
 * Verifies Bully election / cluster-membership state for all 5 independent
 * clusters (each cluster elects on its own -- there is no global election).
 *
 * Usage: run BEFORE killing a leader to see the initial topology, and again
 * AFTER killing one (see docs/MANUAL_DEMONSTRATION.md) to see the new
 * coordinator that Bully elected. This test only OBSERVES via RMI
 * (ping/getRole/getServerId) -- it never kills or restarts a process itself.
 *
 * Env overrides (comma "id:host:port" triples) mirror the Manager's own
 * discovery config; sensible localhost defaults are used otherwise.
 */
public class BullyElectionTest {

    private static final LogicalClock clock = new LogicalClock();

    public static void main(String[] args) throws Exception {
        System.out.println("============================================================");
        System.out.println("  BULLY ELECTION / CLUSTER TOPOLOGY TEST");
        System.out.println("============================================================");

        int failures = 0;
        failures += checkCluster("ChargingStation", "ChargingStationService",
                env("STATION_INSTANCES", "1:localhost:1234,2:localhost:1244,3:localhost:1254"));
        failures += checkCluster("Reservation", "Reservation",
                env("RESERVATION_INSTANCES", "3:localhost:1235,1:localhost:1245,2:localhost:1255"));
        failures += checkCluster("ChargingSession", "ChargingSessionService",
                env("SESSION_INSTANCES", "1:localhost:1236,2:localhost:1246,3:localhost:1256"));
        failures += checkCluster("Pricing", "PricingService",
                env("PRICING_INSTANCES", "1:localhost:1238,2:localhost:1248,3:localhost:1258"));
        failures += checkCluster("Payment", "PaymentService",
                env("PAYMENT_INSTANCES", "1:localhost:1237,2:localhost:1247,3:localhost:1257"));

        System.out.println("============================================================");
        if (failures == 0) {
            System.out.println("RESULT: PASS -- every reachable cluster has exactly one PRIMARY.");
        } else {
            System.out.println("RESULT: " + failures + " cluster(s) did not have exactly one reachable PRIMARY "
                    + "(expected immediately after a kill, before the next election completes).");
        }
        System.out.println("============================================================");
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v != null && !v.trim().isEmpty()) ? v.trim() : def;
    }

    private static int checkCluster(String label, String bindPrefix, String instancesCsv) {
        System.out.println("\n--- " + label + " cluster (Bully election independent of all others) ---");
        int primaries = 0;
        int reachable = 0;
        for (String entry : instancesCsv.split(",")) {
            String[] parts = entry.trim().split(":");
            if (parts.length != 3) continue;
            int id = Integer.parseInt(parts[0]);
            String host = parts[1];
            int port = Integer.parseInt(parts[2]);
            String url = "rmi://" + host + ":" + port + "/" + bindPrefix + "-" + id;
            try {
                ClusterNodeInterface node = (ClusterNodeInterface) Naming.lookup(url);
                long sendL = clock.sendEvent();
                LamportResult<Boolean> pingRes = node.ping(sendL);
                clock.receiveEvent(pingRes.getTimestamp());

                long rL = clock.sendEvent();
                LamportResult<String> roleRes = node.getRole(rL);
                clock.receiveEvent(roleRes.getTimestamp());

                reachable++;
                if ("PRIMARY".equals(roleRes.getData())) primaries++;
                System.out.println("  [BULLY] " + bindPrefix + "-" + id + " @ " + host + ":" + port
                        + " -> ALIVE, role=" + roleRes.getData());
            } catch (Exception e) {
                System.out.println("  [BULLY] " + bindPrefix + "-" + id + " @ " + host + ":" + port
                        + " -> UNREACHABLE (" + e.getClass().getSimpleName() + ")");
            }
        }
        System.out.println("  Summary: " + reachable + " reachable, " + primaries + " reporting PRIMARY.");
        return (reachable > 0 && primaries == 1) ? 0 : 1;
    }
}
