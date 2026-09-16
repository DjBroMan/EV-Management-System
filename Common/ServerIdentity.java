package Common;

import java.util.ArrayList;
import java.util.List;

/**
 * Parses the environment variables that let a single server implementation
 * (ChargingStationServer, ReservationServer, ChargingSessionServer,
 * PricingServer, PaymentServer) run as one of N distinct cluster instances.
 *
 * Env vars (all optional, sensible single-instance defaults preserved so the
 * original 1-instance-per-service behavior still works if unset):
 *
 *   SERVER_ID          unique small integer within this service's cluster.
 *                       Higher ID = higher Bully election priority. Default 1.
 *   SERVER_ROLE         "PRIMARY" or "SECONDARY" (initial role only -- Bully
 *                       election can change this at runtime). Default PRIMARY.
 *   RMI_REGISTRY_PORT   this instance's own RMI registry port.
 *   RMI_EXPORT_PORT     this instance's own RMI export (UnicastRemoteObject) port.
 *   SERVICE_NAME        logical service name used in RMI bound names / logs,
 *                       e.g. "ChargingStationService".
 *   PEERS               comma-separated list of every OTHER instance in this
 *                       same cluster, format "id:host:port,id:host:port,..."
 *                       (registry port of the peer, not the export port).
 */
public class ServerIdentity {

    public final int serverId;
    public final String role;
    public final int registryPort;
    public final int exportPort;
    public final String serviceName;
    public final List<PeerHandle> peers;

    public ServerIdentity(int serverId, String role, int registryPort, int exportPort,
                            String serviceName, List<PeerHandle> peers) {
        this.serverId = serverId;
        this.role = role;
        this.registryPort = registryPort;
        this.exportPort = exportPort;
        this.serviceName = serviceName;
        this.peers = peers;
    }

    public static ServerIdentity fromEnvironment(String serviceName, int defaultRegistryPort,
                                                  int defaultExportPort, String defaultRole) {
        int id = parseIntEnv("SERVER_ID", 1);
        String role = System.getenv("SERVER_ROLE");
        if (role == null || role.trim().isEmpty()) {
            role = defaultRole;
        }
        int regPort = parseIntEnv("RMI_REGISTRY_PORT", defaultRegistryPort);
        int expPort = parseIntEnv("RMI_EXPORT_PORT", defaultExportPort);

        String name = System.getenv("SERVICE_NAME");
        if (name == null || name.trim().isEmpty()) {
            name = serviceName;
        }

        List<PeerHandle> peers = new ArrayList<>();
        String peersEnv = System.getenv("PEERS");
        if (peersEnv != null && !peersEnv.trim().isEmpty()) {
            for (String entry : peersEnv.split(",")) {
                entry = entry.trim();
                if (entry.isEmpty()) continue;
                String[] parts = entry.split(":");
                if (parts.length != 3) continue;
                try {
                    int peerId = Integer.parseInt(parts[0].trim());
                    String host = parts[1].trim();
                    int port = Integer.parseInt(parts[2].trim());
                    peers.add(new PeerHandle(peerId, host, port));
                } catch (NumberFormatException ignored) {
                    // malformed PEERS entry, skip
                }
            }
        }

        return new ServerIdentity(id, role.toUpperCase(), regPort, expPort, name, peers);
    }

    private static int parseIntEnv(String key, int defaultValue) {
        String v = System.getenv(key);
        if (v == null || v.trim().isEmpty()) return defaultValue;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
