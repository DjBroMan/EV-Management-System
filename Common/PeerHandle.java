package Common;

/**
 * Identifies one sibling instance within the same service cluster
 * (e.g. ChargingStationServer-2), by its Bully server ID and its own
 * RMI registry host/port. Used both by BullyElection (peer messaging)
 * and by the Manager (static per-cluster instance discovery).
 */
public class PeerHandle {
    public final int id;
    public final String host;
    public final int registryPort;

    public PeerHandle(int id, String host, int registryPort) {
        this.id = id;
        this.host = host;
        this.registryPort = registryPort;
    }

    @Override
    public String toString() {
        return id + "@" + host + ":" + registryPort;
    }
}
