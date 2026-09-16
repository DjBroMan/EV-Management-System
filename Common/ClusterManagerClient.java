package Common;

import java.rmi.Naming;
import Clock.LogicalClock;
import Clock.LamportResult;

/**
 * Thin helper used by ChargingStationServer / ChargingSessionServer /
 * PaymentServer PRIMARY instances to ask the Manager to fan a single
 * replicated change out to the rest of that instance's cluster. Best-effort:
 * if the Manager is unreachable the write already succeeded locally and is
 * logged as a replication warning, mirroring the original Reservation
 * Primary -> Manager -> Secondary behavior on Manager unavailability.
 */
public class ClusterManagerClient {

    public void replicate(String serviceName, int originServerId, StateDelta delta, LogicalClock clock) {
        try {
            String host = getEnv("MANAGER_HOST", "localhost");
            int port = Integer.parseInt(getEnv("MANAGER_PORT", "1240"));
            String url = "rmi://" + host + ":" + port + "/ClusterManager";
            ClusterManagerInterface mgr = (ClusterManagerInterface) Naming.lookup(url);
            long sendL = clock.sendEvent();
            LamportResult<Boolean> res = mgr.replicateUpdate(serviceName, originServerId, delta, sendL);
            if (res != null) {
                clock.receiveEvent(res.getTimestamp());
            }
        } catch (Exception e) {
            System.out.println("[REPLICATION] WARNING: Manager unreachable for fan-out of " + delta
                    + ": " + e.getMessage());
        }
    }

    private static String getEnv(String key, String defaultValue) {
        String v = System.getenv(key);
        return (v != null && !v.trim().isEmpty()) ? v.trim() : defaultValue;
    }
}
