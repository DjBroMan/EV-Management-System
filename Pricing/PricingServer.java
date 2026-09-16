import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.io.Serializable;
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

// RMI Server implementation for dynamic EV charging price computation.
// Runs as one of N instances in the Pricing cluster (P1/P2/P3). Pricing
// tariffs are not mutated by any client-facing call in this workflow, so
// every instance is a read replica loaded independently from its own DB;
// PRIMARY/SECONDARY role and Bully election are still implemented (per the
// distributed-system roadmap) for cluster-membership/health/demo purposes,
// but no write ever depends on which instance currently holds the role.
public class PricingServer extends UnicastRemoteObject
        implements PricingInterface, ClusterNodeInterface {

    private static final long serialVersionUID = 1L;
    private static final double BASE_PRICE = 10.0;

    private final LogicalClock logicalClock = new LogicalClock();
    private final ServerIdentity identity;
    private volatile String role;
    private BullyElection election;

    private Map<String, String> stationDemand;

    // Database access object — null when DB is not configured or unavailable
    private PricingDAO dao = null;

    protected PricingServer(ServerIdentity identity) throws RemoteException {
        super(identity.exportPort);
        this.identity = identity;
        this.role = identity.role;

        stationDemand = new HashMap<>();
        stationDemand.put("S01", "LOW");
        stationDemand.put("S02", "MEDIUM");
        stationDemand.put("S03", "HIGH");
    }

    private String serverName() {
        return "PricingServer-" + identity.serverId + "[" + role + "]";
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
            this.dao = new PricingDAO();
            dao.initTariffsIfEmpty(stationDemand, BASE_PRICE);
            Map<String, String> dbTariffs = dao.loadAllTariffs();
            if (!dbTariffs.isEmpty()) {
                stationDemand = dbTariffs;
            }
            System.out.println("[DB:" + serverName() + "] Database initialized. Loaded "
                    + stationDemand.size() + " tariff records.");
        } catch (Exception e) {
            System.out.println("[DB:" + serverName() + "] WARNING: Database init failed: " + e.getMessage()
                    + ". Continuing without DB persistence.");
            this.dao = null;
        }
    }

    // ---------------------------------------------------------------
    // Bully election + cluster membership wiring
    // ---------------------------------------------------------------

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
                () -> { this.role = "PRIMARY"; log("BULLY", "This instance is now PRIMARY (coordinator) of the Pricing cluster."); },
                (leader) -> { this.role = "SECONDARY"; log("BULLY", "Learned new coordinator: Pricing-" + leader.id); });

        election.setSelfEndpoint(selfHost, identity.registryPort);
        if (initialLeader != null) {
            election.setInitialCoordinator(initialLeader);
        }
        log("LOCAL", "Cluster initialized. Server ID=" + identity.serverId + ", Initial Role=" + role
                + ", Peers=" + peers);
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
            log("LOCAL",
                    "Clock synchronization completed successfully. Calculated offset: " + res.clockOffsetMs + " ms");
            return "Clock synchronized successfully. Offset: " + res.clockOffsetMs + " ms";
        } else {
            log("LOCAL", "Clock synchronization failed: " + res.errorMessage);
            return "Clock synchronization failed: " + res.errorMessage;
        }
    }

    // =========================================================
    // CALCULATE PRICE
    // =========================================================

    @Override
    public double calculatePrice(String stationId, double energyConsumed) throws RemoteException {
        return calculatePrice(stationId, energyConsumed, 0).getData();
    }

    @Override
    public LamportResult<Double> calculatePrice(
            String stationId,
            double energyConsumed,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "CALCULATE PRICE request received for Station " + stationId + ", Energy " + energyConsumed
                + " kWh (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(300);

        if (energyConsumed < 0) {
            log("LOCAL", "Invalid energy value.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(-1.0, respL);
        }

        simulateProcessing(500);

        String demand = stationDemand.get(stationId);
        if (demand == null) {
            demand = "LOW";
        }

        simulateProcessing(400);

        double multiplier;
        switch (demand) {
            case "LOW":
                multiplier = 1.0;
                break;
            case "MEDIUM":
                multiplier = 1.25;
                break;
            case "HIGH":
                multiplier = 1.50;
                break;
            default:
                multiplier = 1.0;
        }

        simulateProcessing(500);
        double finalPrice = BASE_PRICE * energyConsumed * multiplier;

        logicalClock.tick();
        log("LOCAL", "Price calculated: " + BASE_PRICE + " * " + energyConsumed + " * " + multiplier + " = Rs. "
                + finalPrice);

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning CALCULATE PRICE response Rs. " + finalPrice + " (Lamport: " + sendL + ")");

        return new LamportResult<>(finalPrice, sendL);
    }

    // =========================================================
    // GET DEMAND MULTIPLIER
    // =========================================================

    @Override
    public double getDemandMultiplier(String stationId) throws RemoteException {
        return getDemandMultiplier(stationId, 0).getData();
    }

    @Override
    public LamportResult<Double> getDemandMultiplier(
            String stationId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET DEMAND MULTIPLIER request received for Station " + stationId + " (Client Lamport: "
                + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        String demand = stationDemand.get(stationId);
        if (demand == null) {
            demand = "LOW";
        }

        simulateProcessing(400);

        double multiplier;
        switch (demand) {
            case "LOW":
                multiplier = 1.0;
                break;
            case "MEDIUM":
                multiplier = 1.25;
                break;
            case "HIGH":
                multiplier = 1.50;
                break;
            default:
                multiplier = 1.0;
        }

        logicalClock.tick();
        log("LOCAL", "Demand multiplier: " + multiplier);

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning GET DEMAND MULTIPLIER response " + multiplier + " (Lamport: " + sendL + ")");

        return new LamportResult<>(multiplier, sendL);
    }

    // =========================================================
    // ClusterNodeInterface: replication + election + health
    // =========================================================

    @Override
    public synchronized LamportResult<Boolean> applyUpdate(StateDelta delta, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "REPLICATION_RECEIVED: " + delta + " (Lamport: " + clientLamport + "). Clock updated to " + recvL);
        if ("DEMAND_UPDATE".equals(delta.opType) && delta.args.length == 2) {
            stationDemand.put((String) delta.args[0], (String) delta.args[1]);
            log("LOCAL", "REPLICATION_APPLIED: demand updated " + delta.args[0] + " -> " + delta.args[1]);
        }
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public synchronized LamportResult<GenericSnapshot> getClusterSnapshot(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        GenericSnapshot snap = new GenericSnapshot();
        snap.put("stationDemand", new HashMap<>(stationDemand));
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(snap, respL);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized LamportResult<Boolean> applyClusterSnapshot(GenericSnapshot snapshot, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        Map<String, String> restored = snapshot.get("stationDemand");
        if (restored != null) {
            stationDemand = new HashMap<>(restored);
            log("LOCAL", "FULL_SYNC_APPLIED: restored " + stationDemand.size() + " tariff entries.");
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
    public LamportResult<Boolean> promoteToPrimary(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        this.role = "PRIMARY";
        log("LOCAL", "ROLE_CHANGED: promoted to PRIMARY.");
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

            ServerIdentity identity = ServerIdentity.fromEnvironment("PricingService", 1238, 2238, "PRIMARY");

            System.out.println("Starting Pricing Server instance " + identity.serverId + "...");

            LocateRegistry.createRegistry(identity.registryPort);

            PricingServer server = new PricingServer(identity);
            server.initWithDatabase();
            server.initCluster(selfHost);

            String bindUrl = "rmi://localhost:" + identity.registryPort + "/" + identity.serviceName + "-" + identity.serverId;
            Naming.rebind(bindUrl, server);

            System.out.println("=================================");
            System.out.println("       PRICING RMI SERVER");
            System.out.println("=================================");
            System.out.println("Server ID: " + identity.serverId);
            System.out.println("Role: " + server.role);
            System.out.println("Registry Port: " + identity.registryPort);
            System.out.println("Bound: " + bindUrl);
            System.out.println("Peers: " + identity.peers);

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Waiting for pricing requests...");
            System.out.println("=================================");

        } catch (Exception e) {
            System.out.println("Server Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
