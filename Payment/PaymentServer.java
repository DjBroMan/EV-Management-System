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

// RMI Server class implementing PaymentInterface. Runs as one of N instances
// in the Payment cluster (Pay1/Pay2/Pay3). PRIMARY accepts makePayment;
// SECONDARY replicas reject writes and serve load-balanced reads
// (getPaymentStatus/getPaymentDetails).
public class PaymentServer
        extends UnicastRemoteObject
        implements PaymentInterface, ClusterNodeInterface {

    private final LogicalClock logicalClock = new LogicalClock();

    private HashMap<String, String> paymentStatus;
    private HashMap<String, String> paymentDetails;
    private int paymentCounter = 1001;

    private static final String STATION_ID = "S01";

    private ChargingSessionInterface chargingSession;
    private PricingInterface pricing;
    private ChargingStationInterface chargingStation;

    private final ServerIdentity identity;
    private volatile String role;
    private BullyElection election;
    private ClusterManagerClient managerClient;

    private PaymentDAO dao = null;

    public PaymentServer(
            ServerIdentity identity,
            ChargingSessionInterface chargingSession,
            PricingInterface pricing,
            ChargingStationInterface chargingStation)
            throws RemoteException {

        super(identity.exportPort);
        this.identity = identity;
        this.role = identity.role;

        paymentStatus = new HashMap<String, String>();
        paymentDetails = new HashMap<String, String>();

        this.chargingSession = chargingSession;
        this.pricing = pricing;
        this.chargingStation = chargingStation;
    }

    private String serverName() {
        return "PaymentServer-" + identity.serverId + "[" + role + "]";
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
            this.dao = new PaymentDAO();
            dao.loadAllPayments(paymentStatus, paymentDetails);
            int maxCounter = dao.getMaxCounter();
            if (maxCounter >= paymentCounter) {
                paymentCounter = maxCounter + 1;
            }
            System.out.println("[DB:" + serverName() + "] Database initialized. Counter set to " + paymentCounter);
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
                () -> { this.role = "PRIMARY"; log("BULLY", "This instance is now PRIMARY (coordinator) of the Payment cluster."); },
                (leader) -> { this.role = "SECONDARY"; log("BULLY", "Learned new coordinator: Payment-" + leader.id); });

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
    // MAKE PAYMENT (PRIMARY-only write)
    // =========================================================

    @Override
    public String makePayment(String sessionId) throws RemoteException {
        return makePayment(sessionId, 0).getData();
    }

    @Override
    public LamportResult<String> makePayment(
            String sessionId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "MAKE PAYMENT request received for Session " + sessionId + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (rejectIfSecondary("makePayment")) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: this instance is SECONDARY. Route to PRIMARY.", respL);
        }

        simulateProcessing(500);

        if (sessionId == null || sessionId.length() == 0) {
            log("LOCAL", "Payment failed: Invalid Session ID.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Invalid Session ID.", respL);
        }

        long sendL1 = logicalClock.sendEvent();
        log("SEND", "Contacting ChargingSessionServer.getSessionStatus for Session " + sessionId + " (Lamport: " + sendL1 + ")");

        LamportResult<String> sessInfoRes;
        try {
            sessInfoRes = chargingSession.getSessionStatus(sessionId, sendL1);
            logicalClock.receiveEvent(sessInfoRes.getTimestamp());
            log("RECEIVE", "ChargingSessionServer getSessionStatus response received (Session Lamport: " + sessInfoRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "ChargingSessionServer is unavailable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: ChargingSessionServer is unavailable (" + e.getMessage() + ").", respL);
        }

        String sessionInfo = sessInfoRes.getData();
        if (sessionInfo == null || sessionInfo.contains("Session not found")) {
            log("LOCAL", "Session not found: " + sessionId);
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: Session " + sessionId + " not found.", respL);
        }

        if (!sessionInfo.contains("COMPLETED")) {
            log("LOCAL", "Session has not completed charging.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: Session " + sessionId + " has not finished charging yet.", respL);
        }

        long sendL2 = logicalClock.sendEvent();
        log("SEND", "Contacting ChargingSessionServer.getEnergyConsumed for Session " + sessionId + " (Lamport: " + sendL2 + ")");

        LamportResult<Double> energyRes;
        try {
            energyRes = chargingSession.getEnergyConsumed(sessionId, sendL2);
            logicalClock.receiveEvent(energyRes.getTimestamp());
            log("RECEIVE", "ChargingSessionServer getEnergyConsumed response: " + energyRes.getData() + " kWh (Session Lamport: " + energyRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "Could not retrieve energy consumed.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: could not retrieve energy consumed (" + e.getMessage() + ").", respL);
        }

        double energy = energyRes.getData();
        if (energy < 0) {
            log("LOCAL", "Invalid energy value received.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: no energy data found for session " + sessionId + ".", respL);
        }

        long sendL3 = logicalClock.sendEvent();
        log("SEND", "Contacting PricingServer.calculatePrice for Station " + STATION_ID + ", Energy " + energy + " kWh (Lamport: " + sendL3 + ")");

        LamportResult<Double> priceRes;
        try {
            priceRes = pricing.calculatePrice(STATION_ID, energy, sendL3);
            logicalClock.receiveEvent(priceRes.getTimestamp());
            log("RECEIVE", "PricingServer calculatePrice response: Rs. " + priceRes.getData() + " (Pricing Lamport: " + priceRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "PricingServer is unavailable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: PricingServer is unavailable (" + e.getMessage() + ").", respL);
        }

        double amount = priceRes.getData();
        if (amount < 0) {
            log("LOCAL", "PricingServer returned invalid price.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: PricingServer returned an invalid price.", respL);
        }

        String paymentId;
        synchronized (this) {
            paymentId = "PAY-" + paymentCounter++;
        }

        String status = "SUCCESS";
        String releaseMessage = "";

        try {
            long sendL4 = logicalClock.sendEvent();
            log("SEND", "Contacting ChargingSessionServer.getSessionPort for Session " + sessionId + " (Lamport: " + sendL4 + ")");

            LamportResult<String> portRes = chargingSession.getSessionPort(sessionId, sendL4);
            logicalClock.receiveEvent(portRes.getTimestamp());
            log("RECEIVE", "ChargingSessionServer getSessionPort response: " + portRes.getData() + " (Session Lamport: " + portRes.getTimestamp() + ")");

            String portId = portRes.getData();
            if (portId != null && !portId.equals("NONE") && chargingStation != null) {
                long sendL5 = logicalClock.sendEvent();
                log("SEND", "Payment SUCCESS. Contacting ChargingStationServer.releasePort for Port " + portId + " (Lamport: " + sendL5 + ")");

                LamportResult<String> releaseRes = chargingStation.releasePort(portId, sendL5);
                logicalClock.receiveEvent(releaseRes.getTimestamp());
                log("RECEIVE", "ChargingStationServer releasePort response: " + releaseRes.getData() + " (Station Lamport: " + releaseRes.getTimestamp() + ")");

                releaseMessage = "\nCharging Port Status: " + releaseRes.getData();
            }
        } catch (Exception e) {
            log("LOCAL", "WARNING: Could not release charging port after payment: " + e.getMessage());
        }

        String details =
                "Payment ID: " + paymentId
                + "\nSession ID: " + sessionId
                + "\nEnergy Consumed: " + energy + " kWh"
                + "\nAmount: Rs. " + amount
                + "\nPayment Status: " + status
                + releaseMessage;

        synchronized (this) {
            paymentStatus.put(paymentId, status);
            paymentDetails.put(paymentId, details);
            logicalClock.tick();
            log("LOCAL", "Payment " + paymentId + " stored successfully.");
        }

        persist(paymentId, sessionId, energy, amount);
        replicate(paymentId, sessionId, energy, amount, details);

        String result = "Payment Successful!\n" + details;
        long respL = logicalClock.sendEvent();
        log("SEND", "Returning MAKE PAYMENT response to client (Lamport: " + respL + ")");

        return new LamportResult<>(result, respL);
    }

    // =========================================================
    // GET PAYMENT STATUS (read-only)
    // =========================================================

    @Override
    public String getPaymentStatus(String paymentId) throws RemoteException {
        return getPaymentStatus(paymentId, 0).getData();
    }

    @Override
    public LamportResult<String> getPaymentStatus(
            String paymentId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET PAYMENT STATUS request for " + paymentId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            if (!paymentStatus.containsKey(paymentId)) {
                log("LOCAL", "Payment not found in memory: " + paymentId + ". Querying DB...");
                if (dao != null) {
                    try {
                        String dbStatus = dao.queryPaymentStatus(paymentId);
                        if (dbStatus != null) {
                            logicalClock.tick();
                            String result = "Payment ID: " + paymentId + "\nPayment Status: " + dbStatus;
                            long respL = logicalClock.sendEvent();
                            log("SEND", "Returning GET PAYMENT STATUS response from DB (Lamport: " + respL + ")");
                            return new LamportResult<>(result, respL);
                        }
                    } catch (Exception dbEx) {
                        log("LOCAL", "[DB] WARNING: DB fallback query failed: " + dbEx.getMessage());
                    }
                }
                long respL = logicalClock.sendEvent();
                return new LamportResult<>("Payment not found.", respL);
            }

            String status = paymentStatus.get(paymentId);
            logicalClock.tick();
            log("LOCAL", "Payment ID " + paymentId + " status: " + status);

            String result = "Payment ID: " + paymentId + "\nPayment Status: " + status;
            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET PAYMENT STATUS response (Lamport: " + respL + ")");
            return new LamportResult<>(result, respL);
        }
    }

    // =========================================================
    // GET PAYMENT DETAILS (read-only)
    // =========================================================

    @Override
    public String getPaymentDetails(String paymentId) throws RemoteException {
        return getPaymentDetails(paymentId, 0).getData();
    }

    @Override
    public LamportResult<String> getPaymentDetails(
            String paymentId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET PAYMENT DETAILS request for " + paymentId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            if (!paymentDetails.containsKey(paymentId)) {
                log("LOCAL", "Payment details not found in memory: " + paymentId + ". Querying DB...");
                if (dao != null) {
                    try {
                        String dbDetails = dao.queryPaymentDetails(paymentId);
                        if (dbDetails != null) {
                            logicalClock.tick();
                            long respL = logicalClock.sendEvent();
                            log("SEND", "Returning GET PAYMENT DETAILS response from DB (Lamport: " + respL + ")");
                            return new LamportResult<>(dbDetails, respL);
                        }
                    } catch (Exception dbEx) {
                        log("LOCAL", "[DB] WARNING: DB fallback query failed: " + dbEx.getMessage());
                    }
                }
                long respL = logicalClock.sendEvent();
                return new LamportResult<>("Payment not found.", respL);
            }

            String details = paymentDetails.get(paymentId);
            logicalClock.tick();
            log("LOCAL", "Payment details retrieved for " + paymentId);

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET PAYMENT DETAILS response (Lamport: " + respL + ")");
            return new LamportResult<>(details, respL);
        }
    }

    private void persist(String paymentId, String sessionId, double energy, double amount) {
        if (dao != null) {
            try {
                dao.insertPayment(paymentId, sessionId, STATION_ID, energy, amount,
                        PhysicalClock.getSynchronizedPhysicalTimeMillis());
                log("LOCAL", "[DB] Payment " + paymentId + " persisted to database.");
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to persist payment " + paymentId + ": " + dbEx.getMessage());
            }
        }
    }

    private void replicate(String paymentId, String sessionId, double energy, double amount, String details) {
        if (managerClient != null) {
            managerClient.replicate(identity.serviceName, identity.serverId,
                    new StateDelta("PAYMENT_INSERT", paymentId, sessionId, energy, amount, details),
                    logicalClock);
        }
    }

    // =========================================================
    // ClusterNodeInterface: replication + election + health
    // =========================================================

    @Override
    public synchronized LamportResult<Boolean> applyUpdate(StateDelta delta, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "REPLICATION_RECEIVED: " + delta + " (Lamport: " + clientLamport + "). Clock updated to " + recvL);
        if ("PAYMENT_INSERT".equals(delta.opType) && delta.args.length == 5) {
            String paymentId = (String) delta.args[0];
            String sessionId = (String) delta.args[1];
            double energy = (Double) delta.args[2];
            double amount = (Double) delta.args[3];
            String details = (String) delta.args[4];
            paymentStatus.put(paymentId, "SUCCESS");
            paymentDetails.put(paymentId, details);
            persist(paymentId, sessionId, energy, amount);
            log("LOCAL", "REPLICATION_APPLIED: payment " + paymentId);
        }
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public synchronized LamportResult<GenericSnapshot> getClusterSnapshot(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        GenericSnapshot snap = new GenericSnapshot();
        snap.put("paymentStatus", new HashMap<>(paymentStatus));
        snap.put("paymentDetails", new HashMap<>(paymentDetails));
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(snap, respL);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized LamportResult<Boolean> applyClusterSnapshot(GenericSnapshot snapshot, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        Map<String, String> restoredStatus = snapshot.get("paymentStatus");
        Map<String, String> restoredDetails = snapshot.get("paymentDetails");
        if (restoredStatus != null) paymentStatus.putAll(restoredStatus);
        if (restoredDetails != null) paymentDetails.putAll(restoredDetails);
        log("LOCAL", "FULL_SYNC_APPLIED: restored " + (restoredStatus == null ? 0 : restoredStatus.size()) + " payments.");
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

            String stationUrl = Common.ManagerRouting.resolveChargingStationUrl();

            ChargingStationInterface chargingStation = null;
            int maxRetries = 10;
            int retryCount = 0;

            System.out.println("Connecting to ChargingStationServer at " + stationUrl + "...");

            while (retryCount < maxRetries) {
                try {
                    chargingStation = (ChargingStationInterface) Naming.lookup(stationUrl);
                    System.out.println("ChargingStationServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for ChargingStationServer... Retry " + retryCount + "/" + maxRetries + "...");
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }

            if (chargingStation == null) {
                System.out.println("Could not connect to ChargingStationServer.");
                return;
            }

            String sessionUrl = Common.ManagerRouting.resolveChargingSessionUrl();

            ChargingSessionInterface chargingSession = null;
            retryCount = 0;

            System.out.println("Connecting to ChargingSessionServer at " + sessionUrl + "...");

            while (retryCount < maxRetries) {
                try {
                    chargingSession = (ChargingSessionInterface) Naming.lookup(sessionUrl);
                    System.out.println("ChargingSessionServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for ChargingSessionServer... Retry " + retryCount + "/" + maxRetries + "...");
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }

            if (chargingSession == null) {
                System.out.println("Could not connect to ChargingSessionServer.");
                return;
            }

            String pricingUrl = Common.ManagerRouting.resolvePricingUrl();

            PricingInterface pricing = null;
            retryCount = 0;

            System.out.println("Connecting to PricingServer at " + pricingUrl + "...");

            while (retryCount < maxRetries) {
                try {
                    pricing = (PricingInterface) Naming.lookup(pricingUrl);
                    System.out.println("PricingServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for PricingServer... Retry " + retryCount + "/" + maxRetries + "...");
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }

            if (pricing == null) {
                System.out.println("Could not connect to PricingServer.");
                return;
            }

            ServerIdentity identity = ServerIdentity.fromEnvironment("PaymentService", 1237, 2237, "PRIMARY");

            PaymentServer server = new PaymentServer(identity, chargingSession, pricing, chargingStation);
            server.initWithDatabase();
            server.setManagerClient(new ClusterManagerClient());

            LocateRegistry.createRegistry(identity.registryPort);

            server.initCluster(selfHost);

            String bindUrl = "rmi://localhost:" + identity.registryPort + "/" + identity.serviceName + "-" + identity.serverId;
            Naming.rebind(bindUrl, server);

            System.out.println("Payment Server instance " + identity.serverId + " bound to registry successfully.");
            System.out.println("Role: " + server.role + " | Bound: " + bindUrl + " | Peers: " + identity.peers);

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Waiting for payment requests...");

        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
