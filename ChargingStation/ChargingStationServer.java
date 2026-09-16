import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.util.HashMap;
import java.util.Map;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.CristianClient;
import Clock.DistributedLogger;
import Clock.LamportResult;
import Common.ServerIdentity;
import Common.PeerHandle;
import Common.BullyElection;
import Common.ClusterNodeInterface;
import Common.StateDelta;
import Common.GenericSnapshot;
import Common.ClusterManagerClient;

// Runs as one of N instances in the ChargingStation cluster (CS1/CS2/CS3).
// One PRIMARY accepts all client-facing writes (reservePort/releasePort/
// reserveAnyAvailablePort/startPortCharging); SECONDARY replicas reject
// writes and serve load-balanced reads. A Bully election (independent of
// every other cluster) elects a new PRIMARY on failure; writes are fanned
// out to peers via the Manager immediately after being applied locally.
public class ChargingStationServer
        extends UnicastRemoteObject
        implements ChargingStationInterface, ClusterNodeInterface {

    private final LogicalClock logicalClock = new LogicalClock();

    private String[] ports = { "P1", "P2", "P3", "P4" };
    private String[] portStatus = { "AVAILABLE", "AVAILABLE", "AVAILABLE", "AVAILABLE" };

    private final ServerIdentity identity;
    private volatile String role;
    private BullyElection election;
    private ClusterManagerClient managerClient; // fan-out helper, wired by main()

    // Database access object — null when DB is not configured or unavailable
    private ChargingStationDAO dao = null;

    public ChargingStationServer(ServerIdentity identity) throws RemoteException {
        super(identity.exportPort);
        this.identity = identity;
        this.role = identity.role;
    }

    /**
     * Convenience single-instance constructor (id=1, PRIMARY, default ports,
     * no peers/Bully election) matching the original pre-cluster behavior.
     * Used by ReplicationTest's local in-process fallback mode.
     */
    public ChargingStationServer() throws RemoteException {
        this(new ServerIdentity(1, "PRIMARY", 1234, 2234, "ChargingStationService", new java.util.ArrayList<>()));
    }

    private String serverName() {
        return "ChargingStationServer-" + identity.serverId + "[" + role + "]";
    }

    public void setManagerClient(ClusterManagerClient managerClient) {
        this.managerClient = managerClient;
    }

    public void initWithDatabase() {
        if (!DBConnectionHelper.isDatabaseConfigured()) {
            System.out.println("[DB:" + serverName() + "] DB_HOST not set. Running without database persistence.");
            return;
        }
        java.sql.Connection probe = DBConnectionHelper.getConnectionWithRetry(serverName(), 15);
        if (probe == null) {
            System.out.println("[DB:" + serverName() + "] Could not connect to DB. Running without persistence.");
            return;
        }
        try { probe.close(); } catch (Exception ignore) {}
        try {
            this.dao = new ChargingStationDAO();
            dao.initPortsIfEmpty(ports, portStatus, "EV-STATION-01",
                    Clock.PhysicalClock.getSynchronizedPhysicalTimeMillis());
            java.util.Map<String, String> dbStatuses = dao.loadAllPorts();
            for (int i = 0; i < ports.length; i++) {
                String dbStatus = dbStatuses.get(ports[i]);
                if (dbStatus != null) {
                    portStatus[i] = dbStatus;
                }
            }
            System.out.println("[DB:" + serverName() + "] Database initialized successfully.");
        } catch (Exception e) {
            System.out.println("[DB:" + serverName() + "] WARNING: Database init failed: " + e.getMessage()
                    + ". Continuing without DB persistence.");
            this.dao = null;
        }
    }

    public void initCluster(String selfHost) {
        java.util.List<PeerHandle> peers = identity.peers;
        int maxId = identity.serverId;
        PeerHandle initialLeader = null;
        for (PeerHandle p : peers) {
            if (p.id > maxId) {
                maxId = p.id;
                initialLeader = p;
            }
        }
        this.role = (initialLeader == null) ? "PRIMARY" : "SECONDARY";

        election = new BullyElection(identity.serverId, identity.serviceName, peers, logicalClock,
                this::log,
                () -> { this.role = "PRIMARY"; log("BULLY", "This instance is now PRIMARY (coordinator) of the ChargingStation cluster."); },
                (leader) -> { this.role = "SECONDARY"; log("BULLY", "Learned new coordinator: ChargingStation-" + leader.id); });

        election.setSelfEndpoint(selfHost, identity.registryPort);
        if (initialLeader != null) {
            election.setInitialCoordinator(initialLeader);
        }
        log("LOCAL", "Cluster initialized. Server ID=" + identity.serverId + ", Initial Role=" + role + ", Peers=" + peers);
        election.startHeartbeatMonitor();
    }

    private void log(String message) {
        DistributedLogger.log(serverName(), logicalClock, message);
    }

    private void log(String eventType, String message) {
        DistributedLogger.log(serverName(), logicalClock, eventType, message);
    }

    private void simulateProcessing(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("Thread interrupted during processing.");
        }
    }

    private int indexOfPort(String portId) {
        for (int i = 0; i < ports.length; i++) {
            if (ports[i].equalsIgnoreCase(portId)) {
                return i;
            }
        }
        return -1;
    }

    private void replicate(String portId, String status) {
        if (managerClient != null) {
            managerClient.replicate(identity.serviceName, identity.serverId, new StateDelta("PORT_STATUS", portId, status), logicalClock);
        }
    }

    private boolean rejectIfSecondary(String opName) {
        if (!"PRIMARY".equals(role)) {
            log("LOCAL", "REJECTED: " + opName + " requires PRIMARY role; this instance is " + role + ".");
            return true;
        }
        return false;
    }

    @Override
    public String synchronizeClock() throws RemoteException {
        logicalClock.tick();
        log("LOCAL", "Initiating Cristian Physical Clock Synchronization...");
        String timeServerHost = System.getenv("TIME_SERVER_HOST");
        if (timeServerHost == null || timeServerHost.trim().isEmpty()) {
            timeServerHost = "localhost";
        }
        String timeServerUrl = "rmi://" + timeServerHost + ":1239/TimeServer";
        CristianClient.SyncResult res = CristianClient.synchronize(serverName(), timeServerUrl);
        logicalClock.tick();
        if (res.success) {
            log("LOCAL", "Clock synchronization completed successfully. Calculated offset: " + res.clockOffsetMs + " ms");
            return "Clock synchronized successfully. Offset: " + res.clockOffsetMs + " ms";
        } else {
            log("LOCAL", "Clock synchronization failed: " + res.errorMessage);
            return "Clock synchronization failed: " + res.errorMessage;
        }
    }

    // =========================================================
    // STATION STATUS (read-only -- safe on any instance)
    // =========================================================

    @Override
    public String getStationStatus() throws RemoteException {
        return getStationStatus(0).getData();
    }

    @Override
    public synchronized LamportResult<String> getStationStatus(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET STATION STATUS request received from client (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        int availablePorts = 0;
        for (String status : portStatus) {
            if (status.equals("AVAILABLE")) {
                availablePorts++;
            }
        }

        logicalClock.tick();
        log("LOCAL", "Available ports: " + availablePorts + "/" + ports.length);

        String result = "Station EV-STATION-01: " + availablePorts + " of " + ports.length + " ports available.";
        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning GET STATION STATUS response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // AVAILABLE PORTS (read-only)
    // =========================================================

    @Override
    public String getAvailablePorts() throws RemoteException {
        return getAvailablePorts(0).getData();
    }

    @Override
    public synchronized LamportResult<String> getAvailablePorts(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET AVAILABLE PORTS request received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        String result = "Available Ports: ";
        boolean found = false;

        for (int i = 0; i < ports.length; i++) {
            if (portStatus[i].equals("AVAILABLE")) {
                result += ports[i] + " ";
                found = true;
            }
        }

        if (!found) {
            result = "No ports are currently available.";
        }

        logicalClock.tick();
        log("LOCAL", "Available ports: " + result);

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning GET AVAILABLE PORTS response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // CHECK PORT (read-only)
    // =========================================================

    @Override
    public String checkPortAvailability(String portId) throws RemoteException {
        return checkPortAvailability(portId, 0).getData();
    }

    @Override
    public synchronized LamportResult<String> checkPortAvailability(String portId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "CHECK PORT request for " + portId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        int i = indexOfPort(portId);
        String result;
        if (i == -1) {
            result = "Port " + portId + " does not exist.";
        } else {
            result = "Port " + ports[i] + " is " + portStatus[i] + ".";
        }

        logicalClock.tick();
        log("LOCAL", "Port status check result: " + result);

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning CHECK PORT response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // RESERVE PORT (PRIMARY-only write)
    // =========================================================

    @Override
    public String reservePort(String portId) throws RemoteException {
        return reservePort(portId, 0).getData();
    }

    @Override
    public synchronized LamportResult<String> reservePort(String portId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "RESERVE PORT request for " + portId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (rejectIfSecondary("reservePort")) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation failed: this instance is SECONDARY. Route to PRIMARY.", respL);
        }

        simulateProcessing(700);

        int i = indexOfPort(portId);
        String result;

        if (i == -1) {
            result = "Port " + portId + " does not exist.";
        } else if (!portStatus[i].equals("AVAILABLE")) {
            result = "Port " + ports[i] + " is currently " + portStatus[i] + " and cannot be reserved.";
        } else {
            portStatus[i] = "RESERVED";
            result = "Port " + ports[i] + " reserved successfully.";
            logicalClock.tick();
            log("LOCAL", "Port " + ports[i] + " status changed to RESERVED.");
            final String reservedPortId = ports[i];
            persist(reservedPortId, "RESERVED");
            replicate(reservedPortId, "RESERVED");
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning RESERVE PORT response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // RELEASE PORT (PRIMARY-only write)
    // =========================================================

    @Override
    public String releasePort(String portId) throws RemoteException {
        return releasePort(portId, 0).getData();
    }

    @Override
    public synchronized LamportResult<String> releasePort(String portId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "RELEASE PORT request for " + portId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (rejectIfSecondary("releasePort")) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Release failed: this instance is SECONDARY. Route to PRIMARY.", respL);
        }

        simulateProcessing(500);

        int i = indexOfPort(portId);
        String result;

        if (i == -1) {
            result = "Port " + portId + " does not exist.";
        } else {
            portStatus[i] = "AVAILABLE";
            result = "Port " + ports[i] + " released successfully. Now AVAILABLE.";
            logicalClock.tick();
            log("LOCAL", "Port " + portId + " status changed to AVAILABLE.");
            final String releasedPortId = ports[i];
            persist(releasedPortId, "AVAILABLE");
            replicate(releasedPortId, "AVAILABLE");
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning RELEASE PORT response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // RESERVE ANY AVAILABLE PORT (PRIMARY-only write)
    // =========================================================

    @Override
    public String reserveAnyAvailablePort() throws RemoteException {
        return reserveAnyAvailablePort(0).getData();
    }

    @Override
    public synchronized LamportResult<String> reserveAnyAvailablePort(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "RESERVE ANY AVAILABLE PORT request received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (rejectIfSecondary("reserveAnyAvailablePort")) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("NONE", respL);
        }

        simulateProcessing(700);

        for (int i = 0; i < ports.length; i++) {
            if (portStatus[i].equals("AVAILABLE")) {
                portStatus[i] = "RESERVED";
                logicalClock.tick();
                log("LOCAL", "Allocated available port " + ports[i] + " -> RESERVED");
                final String allocatedPortId = ports[i];
                persist(allocatedPortId, "RESERVED");
                replicate(allocatedPortId, "RESERVED");

                long sendL = logicalClock.sendEvent();
                log("SEND", "Returning allocated port " + allocatedPortId + " (Lamport: " + sendL + ")");
                return new LamportResult<>(allocatedPortId, sendL);
            }
        }

        logicalClock.tick();
        log("LOCAL", "No available charging ports found.");

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning NONE (Lamport: " + sendL + ")");

        return new LamportResult<>("NONE", sendL);
    }

    // =========================================================
    // START CHARGING (PRIMARY-only write)
    // =========================================================

    @Override
    public String startPortCharging(String portId) throws RemoteException {
        return startPortCharging(portId, 0).getData();
    }

    @Override
    public synchronized LamportResult<String> startPortCharging(String portId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "START PORT CHARGING request for " + portId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (rejectIfSecondary("startPortCharging")) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("PORT_NOT_FOUND", respL);
        }

        simulateProcessing(700);

        int i = indexOfPort(portId);
        String result;

        if (i == -1) {
            result = "PORT_NOT_FOUND";
        } else if (!portStatus[i].equals("RESERVED")) {
            result = "PORT_NOT_RESERVED";
        } else {
            portStatus[i] = "CHARGING";
            result = "CHARGING_STARTED";
            logicalClock.tick();
            log("LOCAL", "Port " + portId + " status changed to CHARGING.");
            persist(portId, "CHARGING");
            replicate(portId, "CHARGING");
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning START PORT CHARGING response " + result + " (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    private void persist(String portId, String status) {
        if (dao != null) {
            try {
                dao.updatePortStatus(portId, status, Clock.PhysicalClock.getSynchronizedPhysicalTimeMillis());
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to persist " + status + " status for port " + portId
                        + ": " + dbEx.getMessage());
            }
        }
    }

    // =========================================================
    // ClusterNodeInterface: replication + election + health
    // =========================================================

    @Override
    public synchronized LamportResult<Boolean> applyUpdate(StateDelta delta, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "REPLICATION_RECEIVED: " + delta + " (Lamport: " + clientLamport + "). Clock updated to " + recvL);
        if ("PORT_STATUS".equals(delta.opType) && delta.args.length == 2) {
            String portId = (String) delta.args[0];
            String status = (String) delta.args[1];
            int i = indexOfPort(portId);
            if (i != -1) {
                portStatus[i] = status;
                persist(portId, status);
                log("LOCAL", "REPLICATION_APPLIED: " + portId + " -> " + status);
            }
        }
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public synchronized LamportResult<GenericSnapshot> getClusterSnapshot(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        GenericSnapshot snap = new GenericSnapshot();
        Map<String, String> statusMap = new HashMap<>();
        for (int i = 0; i < ports.length; i++) statusMap.put(ports[i], portStatus[i]);
        snap.put("portStatus", (java.io.Serializable) statusMap);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(snap, respL);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized LamportResult<Boolean> applyClusterSnapshot(GenericSnapshot snapshot, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        Map<String, String> restored = snapshot.get("portStatus");
        if (restored != null) {
            for (int i = 0; i < ports.length; i++) {
                String s = restored.get(ports[i]);
                if (s != null) {
                    portStatus[i] = s;
                    persist(ports[i], s);
                }
            }
            log("LOCAL", "FULL_SYNC_APPLIED: restored " + restored.size() + " port statuses.");
        }
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<Boolean> ping(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public synchronized LamportResult<Boolean> promoteToPrimary(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        this.role = "PRIMARY";
        log("LOCAL", "ROLE_CHANGED: promoted to PRIMARY. Now accepting client writes.");
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<String> getRole(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(role, respL);
    }

    @Override
    public LamportResult<Integer> getServerId(long clientLamport) throws RemoteException {
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(identity.serverId, respL);
    }

    @Override
    public LamportResult<Boolean> receiveElection(int candidateId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        boolean ok = election.handleElection(candidateId);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(ok, respL);
    }

    @Override
    public LamportResult<Boolean> receiveOk(int fromId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        election.handleOk(fromId);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<Boolean> receiveCoordinator(int leaderId, String leaderHost, int leaderRegistryPort, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        election.handleCoordinator(leaderId, leaderHost, leaderRegistryPort);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    // =========================================================
    // MAIN
    // =========================================================

    public static void main(String[] args) {
        try {
            String rmiHost = System.getenv("RMI_SERVER_HOST");
            if (rmiHost != null && !rmiHost.trim().isEmpty()) {
                System.setProperty("java.rmi.server.hostname", rmiHost);
            }
            String selfHost = (rmiHost != null && !rmiHost.trim().isEmpty()) ? rmiHost.trim() : "localhost";
            Common.NetworkSetup.installBoundedConnectTimeout();

            ServerIdentity identity = ServerIdentity.fromEnvironment("ChargingStationService", 1234, 2234, "PRIMARY");

            LocateRegistry.createRegistry(identity.registryPort);

            ChargingStationServer server = new ChargingStationServer(identity);
            server.initWithDatabase();
            server.setManagerClient(new ClusterManagerClient());
            server.initCluster(selfHost);

            String bindUrl = "rmi://localhost:" + identity.registryPort + "/" + identity.serviceName + "-" + identity.serverId;
            Naming.rebind(bindUrl, server);

            System.out.println("Charging Station Server instance " + identity.serverId + " is running...");
            System.out.println("Station: EV-STATION-01");
            System.out.println("Role: " + server.role);
            System.out.println("RMI Registry running on port " + identity.registryPort + ".");
            System.out.println("Bound: " + bindUrl);
            System.out.println("Peers: " + identity.peers);

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Waiting for client requests...");
        } catch (Exception e) {
            System.out.println("Server Exception: " + e);
        }
    }
}
