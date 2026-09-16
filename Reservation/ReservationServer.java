import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
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
import java.util.HashMap;

// RMI Server class that implements both ReservationInterface and ReservationReplicationInterface
// Supports both PRIMARY and SECONDARY roles using the same implementation.
//
// The original 2-node (Primary :1235 / Secondary :1245) replication path via
// ReservationReplicationInterface + ReservationServerManager is preserved
// completely unchanged below, so existing ReplicationTest scenarios keep
// passing exactly as before. ClusterNodeInterface is layered on top so a
// 3rd node (and beyond) can participate in a real per-cluster Bully
// election alongside R1/R2, per the distributed-system roadmap.
public class ReservationServer extends UnicastRemoteObject
        implements ReservationInterface, ReservationReplicationInterface, ClusterNodeInterface {

    private static final long serialVersionUID = 1L;

    public enum Role {
        PRIMARY,
        SECONDARY
    }

    private volatile Role role;
    private final int registryPort;
    private final int exportPort;
    private final String serverName;

    private final LogicalClock logicalClock = new LogicalClock();

    // Bully election membership -- optional; only populated when the server
    // is started via ServerIdentity.fromEnvironment() with SERVER_ID/PEERS
    // set (the 3-node-and-beyond path). The original 2-node primary/backup
    // fields above keep working even when this is null.
    private ServerIdentity identity;
    private BullyElection election;

    // In-memory state: reservation ID -> reservation details string
    private final Map<String, String> reservations = new HashMap<>();

    // In-memory state: reservation ID -> charging port ID
    private final Map<String, String> reservationPorts = new HashMap<>();

    // Sequential counter for reservation IDs
    private int reservationCounter = 1001;

    // Database access object — null when DB is not configured or unavailable
    private ReservationDAO dao = null;

    // Remote reference to ChargingStationServer (used in PRIMARY role)
    private ChargingStationInterface chargingStation;

    // Remote reference to ReservationServerManager (used in PRIMARY role for
    // replication)
    private ReservationManagerInterface manager;

    // Constructor with default PRIMARY role (Port 1235 / Export 2235)
    public ReservationServer(ChargingStationInterface chargingStation) throws RemoteException {
        this(chargingStation, Role.PRIMARY, 1235, 2235);
    }

    // Full constructor
    public ReservationServer(
            ChargingStationInterface chargingStation,
            Role role,
            int registryPort,
            int exportPort) throws RemoteException {

        super(exportPort);
        this.chargingStation = chargingStation;
        this.role = role;
        this.registryPort = registryPort;
        this.exportPort = exportPort;
        this.serverName = "ReservationServer[" + role + ":" + registryPort + "]";
    }

    public Role getRole() {
        return role;
    }

    public void setManager(ReservationManagerInterface manager) {
        this.manager = manager;
    }

    public void setChargingStation(ChargingStationInterface chargingStation) {
        this.chargingStation = chargingStation;
    }

    private void log(String message) {
        DistributedLogger.log(serverName, logicalClock, message);
    }

    private void log(String eventType, String message) {
        DistributedLogger.log(serverName, logicalClock, eventType, message);
    }

    private void simulateProcessing(long milliseconds) {
        try {
            Thread.sleep(milliseconds);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log("Thread interrupted during processing.");
        }
    }

    /**
     * Connects to the MySQL database (primary or secondary instance selected by
     * DB_NAME env var), loads all existing reservations into the in-memory maps,
     * and recovers the reservation counter from MAX(reservation_id).
     */
    public void initWithDatabase() {
        if (!DBConnectionHelper.isDatabaseConfigured()) {
            System.out.println("[DB:" + serverName + "] DB_HOST not set. Running without database persistence.");
            return;
        }
        java.sql.Connection probe = DBConnectionHelper.getConnectionWithRetry(serverName, 15);
        if (probe == null) {
            System.out.println("[DB:" + serverName + "] Could not connect to DB. Running without persistence.");
            return;
        }
        try { probe.close(); } catch (Exception ignore) {}
        try {
            this.dao = new ReservationDAO();
            // Recover in-memory maps from DB
            Map<String, String> dbReservations = dao.loadAllReservations();
            Map<String, String> dbPorts        = dao.loadAllReservationPorts();
            synchronized (this) {
                reservations.putAll(dbReservations);
                reservationPorts.putAll(dbPorts);
                int maxCounter = dao.getMaxCounter();
                if (maxCounter >= reservationCounter) {
                    reservationCounter = maxCounter + 1;
                }
            }
            System.out.println("[DB:" + serverName + "] Database initialized. Loaded "
                    + dbReservations.size() + " reservations. Counter set to " + reservationCounter);
        } catch (Exception e) {
            System.out.println("[DB:" + serverName + "] WARNING: Database init failed: " + e.getMessage()
                    + ". Continuing without DB persistence.");
            this.dao = null;
        }
    }

    /**
     * Parses a field value from the reservation details string.
     * Format: "Reservation ID: RES1001, User ID: u1, Vehicle ID: EV-1, Port: P1, Status: CONFIRMED"
     */
    private static String extractFromDetails(String details, String key) {
        if (details == null) return "";
        String marker = key + ": ";
        int start = details.indexOf(marker);
        if (start == -1) return "";
        start += marker.length();
        int end = details.indexOf(",", start);
        return (end == -1 ? details.substring(start) : details.substring(start, end)).trim();
    }

    private ReservationManagerInterface lookupManager() {
        if (this.manager != null) {
            return this.manager;
        }
        try {
            String mgrHost = System.getenv("MANAGER_HOST");
            if (mgrHost == null || mgrHost.trim().isEmpty()) {
                mgrHost = "localhost";
            }
            String mgrPortStr = System.getenv("MANAGER_PORT");
            int mgrPort = (mgrPortStr != null && !mgrPortStr.trim().isEmpty())
                    ? Integer.parseInt(mgrPortStr.trim())
                    : 1240;

            String mgrUrl = "rmi://" + mgrHost + ":" + mgrPort + "/ReservationManager";
            ReservationManagerInterface mgr = (ReservationManagerInterface) Naming.lookup(mgrUrl);
            this.manager = mgr;
            return mgr;
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Resolves the ChargingStation endpoint to connect to. Since the
     * ChargingStation cluster now has N instances with a Bully-elected
     * leader (no single instance is guaranteed to keep answering at a fixed
     * port), the default path routes through the Manager's load-balanced
     * "ChargingStationService" proxy, exactly like every other cross-service
     * call in the system. STATION_URL remains available as an explicit
     * override for direct single-instance manual testing.
     */
    static String resolveStationUrl() {
        return Common.ManagerRouting.resolveChargingStationUrl();
    }

    private void ensureChargingStationConnected() {
        if (this.chargingStation != null)
            return;

        String stationUrl = resolveStationUrl();

        try {
            this.chargingStation = (ChargingStationInterface) Naming.lookup(stationUrl);
            log("LOCAL", "Connected to ChargingStationServer at " + stationUrl);
        } catch (Exception e) {
            log("LOCAL",
                    "Warning: Could not connect to ChargingStationServer at " + stationUrl + ": " + e.getMessage());
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
        CristianClient.SyncResult res = CristianClient.synchronize(serverName, timeServerUrl);
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
    // CLIENT INTERFACE: RESERVE SLOT
    // =========================================================

    @Override
    public String reserveSlot(String userId, String vehicleId) throws RemoteException {
        return reserveSlot(userId, vehicleId, 0).getData();
    }

    @Override
    public LamportResult<String> reserveSlot(
            String userId,
            String vehicleId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "RESERVE SLOT request received from User " + userId + ", Vehicle " + vehicleId
                + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        // Role check: Only PRIMARY can process client reservations
        if (this.role == Role.SECONDARY) {
            logicalClock.tick();
            log("LOCAL", "REJECTED: Server is in SECONDARY role. Requests must be directed to PRIMARY.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(
                    "Reservation failed: Server is in SECONDARY backup mode. Direct request to PRIMARY.", respL);
        }

        simulateProcessing(500);

        if (userId == null || userId.trim().isEmpty()) {
            log("LOCAL", "Reservation failed: Invalid User ID.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation failed: Invalid User ID.", respL);
        }

        if (vehicleId == null || vehicleId.trim().isEmpty()) {
            log("LOCAL", "Reservation failed: Invalid Vehicle ID.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation failed: Invalid Vehicle ID.", respL);
        }

        ensureChargingStationConnected();
        if (chargingStation == null) {
            log("LOCAL", "ChargingStationServer is unavailable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation failed: ChargingStationServer is unavailable.", respL);
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "Contacting ChargingStationServer.reserveAnyAvailablePort (Lamport: " + sendL + ")");

        LamportResult<String> stationRes;
        try {
            stationRes = chargingStation.reserveAnyAvailablePort(sendL);
            logicalClock.receiveEvent(stationRes.getTimestamp());
            log("RECEIVE", "ChargingStationServer returned port " + stationRes.getData() + " (Station Lamport: "
                    + stationRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "ChargingStationServer communication error: " + e.getMessage());
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(
                    "Reservation failed: ChargingStationServer is unavailable (" + e.getMessage() + ").", respL);
        }

        String portId = stationRes.getData();
        if (portId == null || portId.equals("NONE")) {
            log("LOCAL", "No charging ports are available.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation failed: No charging ports available.", respL);
        }

        log("LOCAL", "Charging port successfully allocated: " + portId + ". Processing reservation...");
        simulateProcessing(700);

        String reservationId;
        int currentCounter;
        String reservationDetails;

        synchronized (this) {
            currentCounter = reservationCounter++;
            reservationId = "RES" + currentCounter;

            reservationDetails = "Reservation ID: " + reservationId +
                    ", User ID: " + userId +
                    ", Vehicle ID: " + vehicleId +
                    ", Port: " + portId +
                    ", Status: CONFIRMED";

            reservations.put(reservationId, reservationDetails);
            reservationPorts.put(reservationId, portId);
            logicalClock.tick();
            log("LOCAL", "STATE_UPDATE: Reservation " + reservationId + " stored locally (" + reservationId + " -> "
                    + portId + "). Counter=" + reservationCounter);
        }

        // SYNCHRONOUS REPLICATION TO MANAGER -> SECONDARY
        ReservationManagerInterface mgr = lookupManager();
        if (mgr != null) {
            try {
                long repSendL = logicalClock.sendEvent();
                log("SEND", "Informing ReservationServerManager to replicate " + reservationId + " -> " + portId
                        + " (Lamport: " + repSendL + ")");
                LamportResult<Boolean> repRes = mgr.replicateReservation(reservationId, reservationDetails, portId,
                        currentCounter, repSendL);
                logicalClock.receiveEvent(repRes.getTimestamp());
                log("RECEIVE",
                        "Replication acknowledged by Manager. Status: " + (repRes.getData() ? "SUCCESS" : "FAILED")
                                + " (Manager Lamport: " + repRes.getTimestamp() + ")");
            } catch (Exception repEx) {
                log("LOCAL", "WARNING: Synchronous replication to Manager failed: " + repEx.getMessage());
            }
        } else {
            log("LOCAL", "Notice: ReservationServerManager not reachable. Operating in standalone/unreplicated mode.");
        }

        // PERSIST TO DATABASE (after replication so memory is always ahead of DB)
        if (dao != null) {
            try {
                dao.insertReservation(reservationId, userId, vehicleId, portId,
                        PhysicalClock.getSynchronizedPhysicalTimeMillis());
                log("LOCAL", "[DB] Reservation " + reservationId + " persisted to database.");
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to persist reservation " + reservationId
                        + ": " + dbEx.getMessage());
            }
        }

        String result = "Reservation successful!\n" + reservationDetails;
        long respL = logicalClock.sendEvent();
        log("SEND", "Returning RESERVE SLOT response to client (Lamport: " + respL + ")");

        return new LamportResult<>(result, respL);
    }

    // =========================================================
    // CLIENT INTERFACE: CANCEL RESERVATION
    // =========================================================

    @Override
    public String cancelReservation(String reservationId) throws RemoteException {
        return cancelReservation(reservationId, 0).getData();
    }

    @Override
    public LamportResult<String> cancelReservation(
            String reservationId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "CANCEL RESERVATION request for ID " + reservationId + " received (Client Lamport: "
                + clientLamport + "). Clock updated to " + recvL);

        if (this.role == Role.SECONDARY) {
            logicalClock.tick();
            log("LOCAL", "REJECTED: Server is in SECONDARY role. Requests must be directed to PRIMARY.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(
                    "Cancellation failed: Server is in SECONDARY backup mode. Direct request to PRIMARY.", respL);
        }

        simulateProcessing(500);

        String portId = null;
        boolean found = false;

        synchronized (this) {
            if (reservations.containsKey(reservationId)) {
                found = true;
                portId = reservationPorts.remove(reservationId);
                reservations.remove(reservationId);
                logicalClock.tick();
                log("LOCAL",
                        "STATE_UPDATE: Reservation " + reservationId + " removed locally. Assigned port: " + portId);
            }
        }

        if (found) {
            // PERSIST CANCELLATION TO DATABASE
            if (dao != null) {
                try {
                    dao.deleteReservation(reservationId);
                    log("LOCAL", "[DB] Reservation " + reservationId + " deleted from database.");
                } catch (Exception dbEx) {
                    log("LOCAL", "[DB] WARNING: Failed to delete reservation " + reservationId
                            + " from DB: " + dbEx.getMessage());
                }
            }

            if (portId != null) {
                ensureChargingStationConnected();
                if (chargingStation != null) {
                    long sendL = logicalClock.sendEvent();
                    log("SEND", "Contacting ChargingStationServer.releasePort for port " + portId + " (Lamport: "
                            + sendL + ")");
                    try {
                        LamportResult<String> releaseRes = chargingStation.releasePort(portId, sendL);
                        logicalClock.receiveEvent(releaseRes.getTimestamp());
                        log("RECEIVE", "ChargingStationServer release response: " + releaseRes.getData()
                                + " (Station Lamport: " + releaseRes.getTimestamp() + ")");
                    } catch (RemoteException e) {
                        log("LOCAL", "WARNING: Could not release port " + portId + " on ChargingStationServer: "
                                + e.getMessage());
                    }
                }
            }

            // SYNCHRONOUS REPLICATION TO MANAGER -> SECONDARY
            ReservationManagerInterface mgr = lookupManager();
            if (mgr != null) {
                try {
                    long repSendL = logicalClock.sendEvent();
                    log("SEND", "Informing ReservationServerManager to replicate CANCEL for " + reservationId
                            + " (Lamport: " + repSendL + ")");
                    LamportResult<Boolean> repRes = mgr.replicateCancellation(reservationId, repSendL);
                    logicalClock.receiveEvent(repRes.getTimestamp());
                    log("RECEIVE",
                            "Cancellation replication acknowledged by Manager. Status: "
                                    + (repRes.getData() ? "SUCCESS" : "FAILED") + " (Manager Lamport: "
                                    + repRes.getTimestamp() + ")");
                } catch (Exception repEx) {
                    log("LOCAL",
                            "WARNING: Synchronous cancellation replication to Manager failed: " + repEx.getMessage());
                }
            }

            String result = "Reservation " + reservationId + " cancelled successfully.";
            long respL = logicalClock.sendEvent();
            log("SEND", "Returning CANCEL RESERVATION response to client (Lamport: " + respL + ")");
            return new LamportResult<>(result, respL);
        }

        String result = "Reservation " + reservationId + " not found.";
        long respL = logicalClock.sendEvent();
        log("SEND", "Returning CANCEL RESERVATION response to client (Lamport: " + respL + ")");
        return new LamportResult<>(result, respL);
    }

    // =========================================================
    // CLIENT INTERFACE: GET RESERVATION
    // =========================================================

    @Override
    public String getReservation(String reservationId) throws RemoteException {
        return getReservation(reservationId, 0).getData();
    }

    @Override
    public LamportResult<String> getReservation(
            String reservationId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET RESERVATION request for ID " + reservationId + " received (Client Lamport: " + clientLamport
                + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            if (reservations.containsKey(reservationId)) {
                String details = reservations.get(reservationId);
                logicalClock.tick();
                log("LOCAL", "Reservation details found in state: " + details);

                long respL = logicalClock.sendEvent();
                log("SEND", "Returning GET RESERVATION response (Lamport: " + respL + ")");
                return new LamportResult<>(details, respL);
            }
        }

        String result = "Reservation " + reservationId + " not found.";
        long respL = logicalClock.sendEvent();
        log("SEND", "Returning GET RESERVATION response (Lamport: " + respL + ")");
        return new LamportResult<>(result, respL);
    }

    // =========================================================
    // CLIENT INTERFACE: GET RESERVATION PORT
    // =========================================================

    @Override
    public String getReservationPort(String reservationId) throws RemoteException {
        return getReservationPort(reservationId, 0).getData();
    }

    @Override
    public LamportResult<String> getReservationPort(
            String reservationId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET RESERVATION PORT request for ID " + reservationId + " received (Client Lamport: "
                + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            String portId = reservationPorts.get(reservationId);
            if (portId == null) {
                portId = "NONE";
            }

            logicalClock.tick();
            log("LOCAL", "Reservation ID " + reservationId + " port mapping in state: " + portId);

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET RESERVATION PORT response " + portId + " (Lamport: " + respL + ")");
            return new LamportResult<>(portId, respL);
        }
    }

    // =========================================================
    // REPLICATION INTERFACE (Manager -> Replica)
    // =========================================================

    @Override
    public LamportResult<Boolean> applyReservationUpdate(
            String reservationId,
            String reservationDetails,
            String portId,
            int currentCounter,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "REPLICATION_RECEIVED: State update for " + reservationId + " -> " + portId
                + " (Manager Lamport: " + clientLamport + "). Clock updated to " + recvL);

        synchronized (this) {
            reservations.put(reservationId, reservationDetails);
            reservationPorts.put(reservationId, portId);
            if (currentCounter >= reservationCounter) {
                reservationCounter = currentCounter + 1;
            }
            logicalClock.tick();
            log("LOCAL", "REPLICATION_APPLIED: Stored " + reservationId + " -> " + portId + ", Counter synchronized to "
                    + reservationCounter);
        }

        // SECONDARY: persist the replicated record to its own DB instance
        if (dao != null) {
            try {
                String userId   = extractFromDetails(reservationDetails, "User ID");
                String vehicleId = extractFromDetails(reservationDetails, "Vehicle ID");
                dao.insertReservation(reservationId, userId, vehicleId, portId,
                        PhysicalClock.getSynchronizedPhysicalTimeMillis());
                log("LOCAL", "[DB] SECONDARY: Replicated reservation " + reservationId + " persisted to DB.");
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to persist replicated reservation " + reservationId
                        + " on secondary: " + dbEx.getMessage());
            }
        }

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning REPLICATION_ACK to Manager (Lamport: " + respL + ")");
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<Boolean> applyCancellationUpdate(
            String reservationId,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "REPLICATION_RECEIVED: Cancellation update for " + reservationId + " (Manager Lamport: "
                + clientLamport + "). Clock updated to " + recvL);

        synchronized (this) {
            reservations.remove(reservationId);
            reservationPorts.remove(reservationId);
            logicalClock.tick();
            log("LOCAL", "REPLICATION_APPLIED: Removed reservation " + reservationId + " from replica state.");
        }

        // SECONDARY: delete the cancelled record from its own DB instance
        if (dao != null) {
            try {
                dao.deleteReservation(reservationId);
                log("LOCAL", "[DB] SECONDARY: Cancelled reservation " + reservationId + " deleted from DB.");
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to delete cancelled reservation " + reservationId
                        + " from secondary DB: " + dbEx.getMessage());
            }
        }

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning CANCELLATION_ACK to Manager (Lamport: " + respL + ")");
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<Boolean> synchronizeFullState(
            ReservationStateSnapshot snapshot,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "FULL_SYNC_RECEIVED: Full state snapshot received (Manager Lamport: " + clientLamport
                + "). Clock updated to " + recvL);

        if (snapshot == null) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }

        synchronized (this) {
            reservations.clear();
            reservationPorts.clear();
            reservations.putAll(snapshot.getReservations());
            reservationPorts.putAll(snapshot.getReservationPorts());
            this.reservationCounter = snapshot.getReservationCounter();

            logicalClock.tick();
            log("LOCAL", "FULL_SYNC_APPLIED: Restored " + reservations.size() + " reservations. Counter set to "
                    + reservationCounter);
        }

        // SECONDARY: rebuild DB to match the new snapshot
        if (dao != null) {
            try {
                dao.deleteAllReservations();
                long ts = PhysicalClock.getSynchronizedPhysicalTimeMillis();
                for (Map.Entry<String, String> entry : snapshot.getReservations().entrySet()) {
                    String resId   = entry.getKey();
                    String details = entry.getValue();
                    String userId   = extractFromDetails(details, "User ID");
                    String vehicleId = extractFromDetails(details, "Vehicle ID");
                    String portId   = snapshot.getReservationPorts().get(resId);
                    if (portId == null) portId = "UNKNOWN";
                    dao.insertReservation(resId, userId, vehicleId, portId, ts);
                }
                log("LOCAL", "[DB] FULL_SYNC: Rebuilt DB with " + snapshot.getReservations().size()
                        + " reservation records.");
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to rebuild DB during full sync: " + dbEx.getMessage());
            }
        }

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning FULL_SYNC_ACK to Manager (Lamport: " + respL + ")");
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<ReservationStateSnapshot> getStateSnapshot(
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE",
                "SNAPSHOT_REQUEST received from Manager (Lamport: " + clientLamport + "). Clock updated to " + recvL);

        ReservationStateSnapshot snapshot;
        synchronized (this) {
            snapshot = new ReservationStateSnapshot(reservations, reservationPorts, reservationCounter);
            logicalClock.tick();
            log("LOCAL", "Created state snapshot: " + snapshot);
        }

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning state snapshot to Manager (Lamport: " + respL + ")");
        return new LamportResult<>(snapshot, respL);
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
        log("RECEIVE",
                "PROMOTION_SIGNAL received from Manager (Lamport: " + clientLamport + "). Clock updated to " + recvL);

        synchronized (this) {
            this.role = Role.PRIMARY;
            ensureChargingStationConnected();
            logicalClock.tick();
            log("LOCAL",
                    "ROLE_CHANGED: Secondary server PROMOTED TO PRIMARY! Now accepting active client reservations.");
        }

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning PROMOTION_ACK to Manager (Lamport: " + respL + ")");
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<String> getRole(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(this.role.name(), respL);
    }

    @Override
    public LamportResult<Boolean> resetState(String targetRole, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        synchronized (this) {
            reservations.clear();
            reservationPorts.clear();
            reservationCounter = 1001;
            if (targetRole != null && !targetRole.trim().isEmpty()) {
                this.role = Role.valueOf(targetRole.trim().toUpperCase());
            }
            logicalClock.tick();
            log("LOCAL", "RESET_STATE: State cleared and role set to " + this.role);
        }

        // Clear the DB to match the reset in-memory state
        if (dao != null) {
            try {
                dao.deleteAllReservations();
                log("LOCAL", "[DB] RESET_STATE: DB reservations table cleared.");
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to clear DB during reset: " + dbEx.getMessage());
            }
        }

        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    // =========================================================
    // BULLY ELECTION (3-node-and-beyond cluster membership)
    // =========================================================

    /**
     * Wires up an independent Bully election for the Reservation cluster.
     * Only called when this instance was started with SERVER_ID/PEERS set
     * (see main()); the original hardcoded 2-node Manager-driven failover
     * continues to work unchanged for R1/R2 regardless of whether this is
     * active, since promoteToPrimary()/getRole() are shared by both paths.
     */
    public void initCluster(ServerIdentity identity, String selfHost) {
        this.identity = identity;
        java.util.List<PeerHandle> peers = identity.peers;
        int maxId = identity.serverId;
        PeerHandle initialLeader = null;
        for (PeerHandle p : peers) {
            if (p.id > maxId) {
                maxId = p.id;
                initialLeader = p;
            }
        }
        this.role = (initialLeader == null) ? Role.PRIMARY : Role.SECONDARY;

        election = new BullyElection(identity.serverId, identity.serviceName, peers, logicalClock,
                this::log,
                () -> {
                    this.role = Role.PRIMARY;
                    ensureChargingStationConnected();
                    log("BULLY", "This instance is now PRIMARY (coordinator) of the Reservation cluster.");
                },
                (leader) -> { this.role = Role.SECONDARY; log("BULLY", "Learned new coordinator: Reservation-" + leader.id); });

        election.setSelfEndpoint(selfHost, identity.registryPort);
        if (initialLeader != null) {
            election.setInitialCoordinator(initialLeader);
        }
        log("LOCAL", "Bully cluster initialized. Server ID=" + identity.serverId + ", Initial Role=" + role
                + ", Peers=" + peers);
        election.startHeartbeatMonitor();
    }

    // =========================================================
    // ClusterNodeInterface (generic replication/election surface, used
    // alongside the typed ReservationReplicationInterface above)
    // =========================================================

    @Override
    public synchronized LamportResult<Boolean> applyUpdate(StateDelta delta, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "REPLICATION_RECEIVED (cluster): " + delta + " (Lamport: " + clientLamport + "). Clock updated to " + recvL);
        if ("RESERVATION_UPSERT".equals(delta.opType) && delta.args.length == 4) {
            String resId = (String) delta.args[0];
            String details = (String) delta.args[1];
            String portId = (String) delta.args[2];
            int counter = (Integer) delta.args[3];
            reservations.put(resId, details);
            reservationPorts.put(resId, portId);
            if (counter >= reservationCounter) reservationCounter = counter + 1;
            if (dao != null) {
                try {
                    String userId = extractFromDetails(details, "User ID");
                    String vehicleId = extractFromDetails(details, "Vehicle ID");
                    dao.insertReservation(resId, userId, vehicleId, portId, PhysicalClock.getSynchronizedPhysicalTimeMillis());
                } catch (Exception ignored) { }
            }
            log("LOCAL", "REPLICATION_APPLIED (cluster): " + resId + " -> " + portId);
        } else if ("RESERVATION_DELETE".equals(delta.opType) && delta.args.length == 1) {
            String resId = (String) delta.args[0];
            reservations.remove(resId);
            reservationPorts.remove(resId);
            if (dao != null) {
                try { dao.deleteReservation(resId); } catch (Exception ignored) { }
            }
            log("LOCAL", "REPLICATION_APPLIED (cluster): removed " + resId);
        }
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public synchronized LamportResult<GenericSnapshot> getClusterSnapshot(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        GenericSnapshot snap = new GenericSnapshot();
        snap.put("reservations", new HashMap<>(reservations));
        snap.put("reservationPorts", new HashMap<>(reservationPorts));
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(snap, respL);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized LamportResult<Boolean> applyClusterSnapshot(GenericSnapshot snapshot, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        Map<String, String> restoredRes = snapshot.get("reservations");
        Map<String, String> restoredPorts = snapshot.get("reservationPorts");
        if (restoredRes != null) reservations.putAll(restoredRes);
        if (restoredPorts != null) reservationPorts.putAll(restoredPorts);
        log("LOCAL", "FULL_SYNC_APPLIED (cluster): restored " + (restoredRes == null ? 0 : restoredRes.size()) + " reservations.");
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<Integer> getServerId(long clientLamport) throws RemoteException {
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(identity != null ? identity.serverId : -1, respL);
    }

    @Override
    public LamportResult<Boolean> receiveElection(int candidateId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        boolean ok = election != null && election.handleElection(candidateId);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(ok, respL);
    }

    @Override
    public LamportResult<Boolean> receiveOk(int fromId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        if (election != null) election.handleOk(fromId);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public LamportResult<Boolean> receiveCoordinator(int leaderId, String leaderHost, int leaderRegistryPort, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        if (election != null) election.handleCoordinator(leaderId, leaderHost, leaderRegistryPort);
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    // =========================================================
    // MAIN ENTRY POINT
    // =========================================================

    public static void main(String[] args) {
        try {
            String rmiHost = System.getenv("RMI_SERVER_HOST");
            if (rmiHost != null && !rmiHost.trim().isEmpty()) {
                System.setProperty("java.rmi.server.hostname", rmiHost);
            }
            Common.NetworkSetup.installBoundedConnectTimeout();

            // Determine role and port from CLI arguments or environment variables
            Role role = Role.PRIMARY;
            int regPort = 1235;
            int expPort = 2235;

            if (args.length > 0) {
                if (args[0].equalsIgnoreCase("secondary")) {
                    role = Role.SECONDARY;
                    regPort = 1245;
                    expPort = 2245;
                } else if (args[0].equalsIgnoreCase("primary")) {
                    role = Role.PRIMARY;
                    regPort = 1235;
                    expPort = 2235;
                }
            }

            if (args.length > 1) {
                try {
                    regPort = Integer.parseInt(args[1]);
                    expPort = regPort + 1000;
                } catch (NumberFormatException ignored) {
                }
            }

            String envRole = System.getenv("RESERVATION_ROLE");
            if (envRole != null && !envRole.trim().isEmpty()) {
                if (envRole.equalsIgnoreCase("SECONDARY")) {
                    role = Role.SECONDARY;
                    regPort = 1245;
                    expPort = 2245;
                } else {
                    role = Role.PRIMARY;
                    regPort = 1235;
                    expPort = 2235;
                }
            }

            String envRegPort = System.getenv("RMI_REGISTRY_PORT");
            if (envRegPort != null && !envRegPort.trim().isEmpty()) {
                regPort = Integer.parseInt(envRegPort.trim());
                expPort = regPort + 1000;
            }

            ChargingStationInterface chargingStation = null;
            if (role == Role.PRIMARY) {
                String stationUrl = resolveStationUrl();

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
                        System.out.println(
                                "Waiting for ChargingStationServer... Retry " + retryCount + "/" + maxRetries + "...");
                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException ie) {
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                }
            }

            try {
                LocateRegistry.createRegistry(regPort);
            } catch (Exception e) {
                // Registry might already exist on this port
            }

            ReservationServer server = new ReservationServer(chargingStation, role, regPort, expPort);

            // Initialize database: load existing reservations, recover counter
            server.initWithDatabase();

            String serviceBindUrl = "rmi://localhost:" + regPort + "/ReservationService";
            String replBindUrl = "rmi://localhost:" + regPort + "/ReservationReplicationService";

            Naming.rebind(serviceBindUrl, server);
            Naming.rebind(replBindUrl, server);

            // Bully cluster membership (3-node-and-beyond). Only activates when
            // SERVER_ID is present in the environment; R1/R2 started the classic
            // way (primary/secondary CLI args only) skip this and keep behaving
            // exactly as before.
            String bindUrl = null;
            if (System.getenv("SERVER_ID") != null && !System.getenv("SERVER_ID").trim().isEmpty()) {
                ServerIdentity identity = ServerIdentity.fromEnvironment("Reservation", regPort, expPort, role.name());
                String selfHost = (rmiHost != null && !rmiHost.trim().isEmpty()) ? rmiHost.trim() : "localhost";
                server.initCluster(identity, selfHost);
                bindUrl = "rmi://localhost:" + regPort + "/" + identity.serviceName + "-" + identity.serverId;
                Naming.rebind(bindUrl, server);
            }

            System.out.println("=================================================");
            System.out.println("   RESERVATION RMI SERVER [" + role + "]");
            System.out.println("=================================================");
            System.out.println("Role: " + role);
            System.out.println("Registry Port: " + regPort);
            System.out.println("Export Port: " + expPort);
            System.out.println("Bound Service: " + serviceBindUrl);
            System.out.println("Bound Replication: " + replBindUrl);
            if (bindUrl != null) {
                System.out.println("Bound Cluster Node: " + bindUrl);
            }

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Server ready and waiting for requests...");
            System.out.println("=================================================");

        } catch (Exception e) {
            System.out.println("Server Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}