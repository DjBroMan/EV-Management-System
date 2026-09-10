import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.CristianClient;
import Clock.DistributedLogger;
import Clock.LamportResult;

/**
 * Reservation Server Manager coordinates primary-backup replication,
 * full state synchronization, health checks, failover promotion,
 * and acts as the SINGLE ENTRY POINT (Proxy / Router) for EVClient.
 *
 * EVClient -> Manager (:1240) -> Current Primary (:1235 or :1245)
 */
public class ReservationServerManager extends UnicastRemoteObject
        implements ReservationManagerInterface {

    private static final long serialVersionUID = 1L;
    private static final String SERVER_NAME = "ReservationServerManager";

    private final LogicalClock logicalClock = new LogicalClock();

    private volatile String primaryHost;
    private volatile int primaryPort;
    private volatile String secondaryHost;
    private volatile int secondaryPort;

    private volatile String currentPrimaryHost;
    private volatile int currentPrimaryPort;

    private final int registryPort;

    public ReservationServerManager(
            String primaryHost,
            int primaryPort,
            String secondaryHost,
            int secondaryPort,
            int registryPort,
            int exportPort) throws RemoteException {

        super(exportPort);
        this.primaryHost = primaryHost;
        this.primaryPort = primaryPort;
        this.secondaryHost = secondaryHost;
        this.secondaryPort = secondaryPort;

        this.currentPrimaryHost = primaryHost;
        this.currentPrimaryPort = primaryPort;
        this.registryPort = registryPort;
    }

    public ReservationServerManager(
            String primaryUrl,
            String secondaryUrl,
            int registryPort,
            int exportPort) throws RemoteException {

        super(exportPort);
        this.registryPort = registryPort;
        parseAndSetEndpoints(primaryUrl, secondaryUrl);
    }

    private void parseAndSetEndpoints(String priUrl, String secUrl) {
        try {
            // e.g. rmi://localhost:1235/ReservationReplicationService
            String pClean = priUrl.replace("rmi://", "");
            String pHostPort = pClean.substring(0, pClean.indexOf('/'));
            String[] pParts = pHostPort.split(":");
            this.primaryHost = pParts[0];
            this.primaryPort = Integer.parseInt(pParts[1]);

            String sClean = secUrl.replace("rmi://", "");
            String sHostPort = sClean.substring(0, sClean.indexOf('/'));
            String[] sParts = sHostPort.split(":");
            this.secondaryHost = sParts[0];
            this.secondaryPort = Integer.parseInt(sParts[1]);
        } catch (Exception e) {
            this.primaryHost = "localhost";
            this.primaryPort = 1235;
            this.secondaryHost = "localhost";
            this.secondaryPort = 1245;
        }
        this.currentPrimaryHost = this.primaryHost;
        this.currentPrimaryPort = this.primaryPort;
    }

    private void log(String message) {
        DistributedLogger.log(SERVER_NAME, logicalClock, message);
    }

    private void log(String eventType, String message) {
        DistributedLogger.log(SERVER_NAME, logicalClock, eventType, message);
    }

    public String getCurrentPrimaryUrl() {
        return "rmi://" + currentPrimaryHost + ":" + currentPrimaryPort + "/ReservationService";
    }

    public String getCurrentPrimaryReplUrl() {
        return "rmi://" + currentPrimaryHost + ":" + currentPrimaryPort + "/ReservationReplicationService";
    }

    public String getSecondaryReplUrl() {
        return "rmi://" + secondaryHost + ":" + secondaryPort + "/ReservationReplicationService";
    }

    private ReservationInterface lookupCurrentPrimaryService() {
        try {
            String url = getCurrentPrimaryUrl();
            return (ReservationInterface) Naming.lookup(url);
        } catch (Exception e) {
            log("LOCAL", "Warning: Current Primary service at " + getCurrentPrimaryUrl() + " is unreachable: " + e.getMessage());
            return null;
        }
    }

    private ReservationReplicationInterface lookupCurrentPrimaryReplication() {
        try {
            String url = getCurrentPrimaryReplUrl();
            return (ReservationReplicationInterface) Naming.lookup(url);
        } catch (Exception e) {
            log("LOCAL", "Warning: Current Primary replication at " + getCurrentPrimaryReplUrl() + " is unreachable: " + e.getMessage());
            return null;
        }
    }

    private ReservationReplicationInterface lookupSecondaryReplication() {
        try {
            String url = getSecondaryReplUrl();
            return (ReservationReplicationInterface) Naming.lookup(url);
        } catch (Exception e) {
            log("LOCAL", "Warning: Secondary replication at " + getSecondaryReplUrl() + " is unreachable: " + e.getMessage());
            return null;
        }
    }

    // =========================================================
    // PROXY FORWARDING (EVCLIENT -> MANAGER -> CURRENT PRIMARY)
    // =========================================================

    @Override
    public String reserveSlot(String userId, String vehicleId) throws RemoteException {
        return reserveSlot(userId, vehicleId, 0).getData();
    }

    @Override
    public LamportResult<String> reserveSlot(
            String userId,
            String vehicleId,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "PROXY_RESERVE_SLOT received from User " + userId + ", Vehicle " + vehicleId
                + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        ReservationInterface primary = lookupCurrentPrimaryService();
        if (primary == null) {
            log("LOCAL", "Current Primary unreachable. Attempting automatic failover...");
            if (performFailoverInternal()) {
                primary = lookupCurrentPrimaryService();
            }
        }

        if (primary == null) {
            log("LOCAL", "ERROR: No reachable ReservationServer found (Primary and Secondary unavailable).");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation failed: Reservation cluster is unavailable.", respL);
        }

        try {
            long sendL = logicalClock.sendEvent();
            log("SEND", "ROUTING: Forwarding reserveSlot to current Primary at " + getCurrentPrimaryUrl() + " (Lamport: " + sendL + ")");
            LamportResult<String> priRes = primary.reserveSlot(userId, vehicleId, sendL);
            logicalClock.receiveEvent(priRes.getTimestamp());
            log("RECEIVE", "Primary returned reservation response (Primary Lamport: " + priRes.getTimestamp() + ")");

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning proxied RESERVE_SLOT response to EVClient (Lamport: " + respL + ")");
            return new LamportResult<>(priRes.getData(), respL);
        } catch (RemoteException re) {
            log("LOCAL", "RemoteException calling Primary during reserveSlot: " + re.getMessage() + ". Attempting failover...");
            if (performFailoverInternal()) {
                primary = lookupCurrentPrimaryService();
                if (primary != null) {
                    long sendL = logicalClock.sendEvent();
                    log("SEND", "ROUTING: Retrying reserveSlot on newly promoted Primary at " + getCurrentPrimaryUrl() + " (Lamport: " + sendL + ")");
                    LamportResult<String> priRes = primary.reserveSlot(userId, vehicleId, sendL);
                    logicalClock.receiveEvent(priRes.getTimestamp());
                    long respL = logicalClock.sendEvent();
                    return new LamportResult<>(priRes.getData(), respL);
                }
            }
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation failed: Primary server unavailable (" + re.getMessage() + ").", respL);
        }
    }

    @Override
    public String cancelReservation(String reservationId) throws RemoteException {
        return cancelReservation(reservationId, 0).getData();
    }

    @Override
    public LamportResult<String> cancelReservation(
            String reservationId,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "PROXY_CANCEL_RESERVATION received for ID " + reservationId
                + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        ReservationInterface primary = lookupCurrentPrimaryService();
        if (primary == null) {
            log("LOCAL", "Current Primary unreachable. Attempting automatic failover...");
            if (performFailoverInternal()) {
                primary = lookupCurrentPrimaryService();
            }
        }

        if (primary == null) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Cancellation failed: Reservation cluster is unavailable.", respL);
        }

        try {
            long sendL = logicalClock.sendEvent();
            log("SEND", "ROUTING: Forwarding cancelReservation to current Primary at " + getCurrentPrimaryUrl() + " (Lamport: " + sendL + ")");
            LamportResult<String> priRes = primary.cancelReservation(reservationId, sendL);
            logicalClock.receiveEvent(priRes.getTimestamp());
            log("RECEIVE", "Primary returned cancellation response (Primary Lamport: " + priRes.getTimestamp() + ")");

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning proxied CANCEL_RESERVATION response to EVClient (Lamport: " + respL + ")");
            return new LamportResult<>(priRes.getData(), respL);
        } catch (RemoteException re) {
            log("LOCAL", "RemoteException calling Primary during cancelReservation: " + re.getMessage() + ". Attempting failover...");
            if (performFailoverInternal()) {
                primary = lookupCurrentPrimaryService();
                if (primary != null) {
                    long sendL = logicalClock.sendEvent();
                    LamportResult<String> priRes = primary.cancelReservation(reservationId, sendL);
                    logicalClock.receiveEvent(priRes.getTimestamp());
                    long respL = logicalClock.sendEvent();
                    return new LamportResult<>(priRes.getData(), respL);
                }
            }
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Cancellation failed: Primary server unavailable (" + re.getMessage() + ").", respL);
        }
    }

    @Override
    public String getReservation(String reservationId) throws RemoteException {
        return getReservation(reservationId, 0).getData();
    }

    @Override
    public LamportResult<String> getReservation(
            String reservationId,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "PROXY_GET_RESERVATION received for ID " + reservationId
                + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        ReservationInterface primary = lookupCurrentPrimaryService();
        if (primary == null) {
            if (performFailoverInternal()) {
                primary = lookupCurrentPrimaryService();
            }
        }

        if (primary == null) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation " + reservationId + " not found (Cluster unavailable).", respL);
        }

        try {
            long sendL = logicalClock.sendEvent();
            log("SEND", "ROUTING: Forwarding getReservation to current Primary at " + getCurrentPrimaryUrl() + " (Lamport: " + sendL + ")");
            LamportResult<String> priRes = primary.getReservation(reservationId, sendL);
            logicalClock.receiveEvent(priRes.getTimestamp());

            long respL = logicalClock.sendEvent();
            return new LamportResult<>(priRes.getData(), respL);
        } catch (RemoteException re) {
            if (performFailoverInternal()) {
                primary = lookupCurrentPrimaryService();
                if (primary != null) {
                    long sendL = logicalClock.sendEvent();
                    LamportResult<String> priRes = primary.getReservation(reservationId, sendL);
                    logicalClock.receiveEvent(priRes.getTimestamp());
                    long respL = logicalClock.sendEvent();
                    return new LamportResult<>(priRes.getData(), respL);
                }
            }
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation " + reservationId + " query failed (" + re.getMessage() + ").", respL);
        }
    }

    @Override
    public String getReservationPort(String reservationId) throws RemoteException {
        return getReservationPort(reservationId, 0).getData();
    }

    @Override
    public LamportResult<String> getReservationPort(
            String reservationId,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "PROXY_GET_RESERVATION_PORT received for ID " + reservationId
                + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        ReservationInterface primary = lookupCurrentPrimaryService();
        if (primary == null) {
            if (performFailoverInternal()) {
                primary = lookupCurrentPrimaryService();
            }
        }

        if (primary == null) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("NONE", respL);
        }

        try {
            long sendL = logicalClock.sendEvent();
            log("SEND", "ROUTING: Forwarding getReservationPort to current Primary at " + getCurrentPrimaryUrl() + " (Lamport: " + sendL + ")");
            LamportResult<String> priRes = primary.getReservationPort(reservationId, sendL);
            logicalClock.receiveEvent(priRes.getTimestamp());

            long respL = logicalClock.sendEvent();
            return new LamportResult<>(priRes.getData(), respL);
        } catch (RemoteException re) {
            if (performFailoverInternal()) {
                primary = lookupCurrentPrimaryService();
                if (primary != null) {
                    long sendL = logicalClock.sendEvent();
                    LamportResult<String> priRes = primary.getReservationPort(reservationId, sendL);
                    logicalClock.receiveEvent(priRes.getTimestamp());
                    long respL = logicalClock.sendEvent();
                    return new LamportResult<>(priRes.getData(), respL);
                }
            }
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("NONE", respL);
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
        CristianClient.SyncResult res = CristianClient.synchronize(SERVER_NAME, timeServerUrl);
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
    // REPLICATION FORWARDING (PRIMARY -> MANAGER -> SECONDARY)
    // =========================================================

    @Override
    public LamportResult<Boolean> replicateReservation(
            String reservationId,
            String reservationDetails,
            String portId,
            int currentCounter,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "STATE_UPDATE received from Primary: " + reservationId + " -> " + portId + " (Primary Lamport: " + clientLamport + "). Clock updated to " + recvL);

        // If current primary is already the secondary, replication is not forwarded back to itself
        if (this.currentPrimaryPort == this.secondaryPort) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(true, respL);
        }

        ReservationReplicationInterface secondary = lookupSecondaryReplication();
        if (secondary == null) {
            log("LOCAL", "REPLICATION_FAILED: Secondary replica is unreachable. Replication aborted.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "REPLICATION_SENT: Forwarding state update for " + reservationId + " -> " + portId + " to Secondary (Lamport: " + sendL + ")");

        try {
            LamportResult<Boolean> secRes = secondary.applyReservationUpdate(reservationId, reservationDetails, portId, currentCounter, sendL);
            logicalClock.receiveEvent(secRes.getTimestamp());
            log("RECEIVE", "Secondary acknowledged reservation replication for " + reservationId + " (Secondary Lamport: " + secRes.getTimestamp() + ")");

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning REPLICATION_CONFIRMED to Primary (Lamport: " + respL + ")");
            return new LamportResult<>(secRes.getData(), respL);
        } catch (Exception e) {
            log("LOCAL", "REPLICATION_ERROR: Failed to replicate to Secondary: " + e.getMessage());
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }
    }

    @Override
    public LamportResult<Boolean> replicateCancellation(
            String reservationId,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "CANCELLATION_UPDATE received from Primary for " + reservationId + " (Primary Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (this.currentPrimaryPort == this.secondaryPort) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(true, respL);
        }

        ReservationReplicationInterface secondary = lookupSecondaryReplication();
        if (secondary == null) {
            log("LOCAL", "CANCELLATION_REPLICATION_FAILED: Secondary replica is unreachable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "REPLICATION_SENT: Forwarding cancellation update for " + reservationId + " to Secondary (Lamport: " + sendL + ")");

        try {
            LamportResult<Boolean> secRes = secondary.applyCancellationUpdate(reservationId, sendL);
            logicalClock.receiveEvent(secRes.getTimestamp());
            log("RECEIVE", "Secondary acknowledged cancellation replication for " + reservationId + " (Secondary Lamport: " + secRes.getTimestamp() + ")");

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning CANCELLATION_CONFIRMED to Primary (Lamport: " + respL + ")");
            return new LamportResult<>(secRes.getData(), respL);
        } catch (Exception e) {
            log("LOCAL", "CANCELLATION_REPLICATION_ERROR: Failed to replicate cancellation: " + e.getMessage());
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }
    }

    // =========================================================
    // FULL STATE SYNCHRONIZATION
    // =========================================================

    @Override
    public LamportResult<Boolean> triggerFullSynchronization(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "FULL_SYNC_REQUEST triggered (Lamport: " + clientLamport + "). Clock updated to " + recvL);

        ReservationReplicationInterface primary = lookupCurrentPrimaryReplication();
        ReservationReplicationInterface secondary = lookupSecondaryReplication();

        if (primary == null) {
            log("LOCAL", "FULL_SYNC_FAILED: Primary is unreachable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }

        if (secondary == null) {
            log("LOCAL", "FULL_SYNC_FAILED: Secondary is unreachable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }

        try {
            long snapSendL = logicalClock.sendEvent();
            log("SEND", "Requesting point-in-time state snapshot from Primary (Lamport: " + snapSendL + ")");
            LamportResult<ReservationStateSnapshot> snapRes = primary.getStateSnapshot(snapSendL);
            logicalClock.receiveEvent(snapRes.getTimestamp());
            ReservationStateSnapshot snapshot = snapRes.getData();
            log("RECEIVE", "Received snapshot from Primary: " + snapshot + " (Primary Lamport: " + snapRes.getTimestamp() + ")");

            long syncSendL = logicalClock.sendEvent();
            log("SEND", "Applying full state snapshot to Secondary replica (Lamport: " + syncSendL + ")");
            LamportResult<Boolean> syncRes = secondary.synchronizeFullState(snapshot, syncSendL);
            logicalClock.receiveEvent(syncRes.getTimestamp());
            log("RECEIVE", "Secondary confirmed full state synchronization (Secondary Lamport: " + syncRes.getTimestamp() + ")");

            long respL = logicalClock.sendEvent();
            log("SEND", "FULL_SYNC_COMPLETED successfully (Lamport: " + respL + ")");
            return new LamportResult<>(true, respL);
        } catch (Exception e) {
            log("LOCAL", "FULL_SYNC_ERROR: Exception during state synchronization: " + e.getMessage());
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }
    }

    // =========================================================
    // FAILOVER & PROMOTION
    // =========================================================

    private synchronized boolean performFailoverInternal() {
        if (this.currentPrimaryPort == this.secondaryPort) {
            // Already failed over to secondary
            return false;
        }

        log("LOCAL", "PRIMARY FAILURE DETECTED! Initiating automated failover promotion to Secondary...");
        ReservationReplicationInterface secondary = lookupSecondaryReplication();
        if (secondary == null) {
            log("LOCAL", "FAILOVER_FAILED: Secondary server at " + getSecondaryReplUrl() + " is also unreachable.");
            return false;
        }

        try {
            long promoSendL = logicalClock.sendEvent();
            log("SEND", "Sending promotion command to Secondary (Lamport: " + promoSendL + ")");
            LamportResult<Boolean> promoRes = secondary.promoteToPrimary(promoSendL);
            logicalClock.receiveEvent(promoRes.getTimestamp());
            log("RECEIVE", "Secondary confirmed promotion to PRIMARY (Lamport: " + promoRes.getTimestamp() + ")");

            // Update Current Primary URL to point to newly promoted Secondary server
            this.currentPrimaryHost = this.secondaryHost;
            this.currentPrimaryPort = this.secondaryPort;

            log("LOCAL", "FAILOVER_COMPLETED: Router updated currentPrimaryUrl -> " + getCurrentPrimaryUrl());
            return true;
        } catch (Exception e) {
            log("LOCAL", "FAILOVER_ERROR: Failed to promote Secondary: " + e.getMessage());
            return false;
        }
    }

    @Override
    public synchronized LamportResult<Boolean> checkAndFailover(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "FAILOVER_CHECK received (Lamport: " + clientLamport + "). Clock updated to " + recvL);

        boolean primaryAlive = false;
        try {
            ReservationReplicationInterface primary = lookupCurrentPrimaryReplication();
            if (primary != null) {
                long pingSendL = logicalClock.sendEvent();
                LamportResult<Boolean> pingRes = primary.ping(pingSendL);
                logicalClock.receiveEvent(pingRes.getTimestamp());

                long roleSendL = logicalClock.sendEvent();
                LamportResult<String> roleRes = primary.getRole(roleSendL);
                logicalClock.receiveEvent(roleRes.getTimestamp());

                primaryAlive = pingRes.getData() && "PRIMARY".equals(roleRes.getData());
            }
        } catch (Exception e) {
            primaryAlive = false;
        }

        if (primaryAlive) {
            log("LOCAL", "Primary is healthy and in PRIMARY role. No failover required.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }

        boolean failoverSuccess = performFailoverInternal();
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(failoverSuccess, respL);
    }

    // =========================================================
    // REPLICATION STATUS INSPECTION
    // =========================================================

    @Override
    public LamportResult<String> getReplicationStatus(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);

        boolean priAlive = false;
        String priRole = "UNKNOWN";
        int priCount = -1;

        boolean secAlive = false;
        String secRole = "UNKNOWN";
        int secCount = -1;

        try {
            String pUrl = "rmi://" + primaryHost + ":" + primaryPort + "/ReservationReplicationService";
            ReservationReplicationInterface primary = (ReservationReplicationInterface) Naming.lookup(pUrl);
            if (primary != null) {
                long pL = logicalClock.sendEvent();
                LamportResult<Boolean> pingRes = primary.ping(pL);
                logicalClock.receiveEvent(pingRes.getTimestamp());
                priAlive = pingRes.getData();

                long rL = logicalClock.sendEvent();
                LamportResult<String> roleRes = primary.getRole(rL);
                logicalClock.receiveEvent(roleRes.getTimestamp());
                priRole = roleRes.getData();

                long sL = logicalClock.sendEvent();
                LamportResult<ReservationStateSnapshot> snapRes = primary.getStateSnapshot(sL);
                logicalClock.receiveEvent(snapRes.getTimestamp());
                priCount = snapRes.getData().getReservations().size();
            }
        } catch (Exception e) {
            priAlive = false;
        }

        try {
            ReservationReplicationInterface secondary = lookupSecondaryReplication();
            if (secondary != null) {
                long pL = logicalClock.sendEvent();
                LamportResult<Boolean> pingRes = secondary.ping(pL);
                logicalClock.receiveEvent(pingRes.getTimestamp());
                secAlive = pingRes.getData();

                long rL = logicalClock.sendEvent();
                LamportResult<String> roleRes = secondary.getRole(rL);
                logicalClock.receiveEvent(roleRes.getTimestamp());
                secRole = roleRes.getData();

                long sL = logicalClock.sendEvent();
                LamportResult<ReservationStateSnapshot> snapRes = secondary.getStateSnapshot(sL);
                logicalClock.receiveEvent(snapRes.getTimestamp());
                secCount = snapRes.getData().getReservations().size();
            }
        } catch (Exception e) {
            secAlive = false;
        }

        String status = "=== REPLICATION CLUSTER & ROUTER STATUS ===\n"
                + "Router Active Primary Target: " + getCurrentPrimaryUrl() + "\n"
                + "Initial Primary Server (" + primaryHost + ":" + primaryPort + "): Status=" + (priAlive ? "ONLINE" : "OFFLINE")
                + ", Role=" + priRole + ", Active Reservations=" + priCount + "\n"
                + "Secondary Server       (" + secondaryHost + ":" + secondaryPort + "): Status=" + (secAlive ? "ONLINE" : "OFFLINE")
                + ", Role=" + secRole + ", Replicated Reservations=" + secCount + "\n"
                + "Cluster Synchronization: " + ((priAlive && secAlive && priCount == secCount) ? "SYNCHRONIZED" : "ATTENTION_REQUIRED");

        long respL = logicalClock.sendEvent();
        return new LamportResult<>(status, respL);
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

            String primaryHost = System.getenv("PRIMARY_HOST");
            if (primaryHost == null || primaryHost.trim().isEmpty()) {
                primaryHost = "localhost";
            }
            int primaryPort = 1235;

            String secondaryHost = System.getenv("SECONDARY_HOST");
            if (secondaryHost == null || secondaryHost.trim().isEmpty()) {
                secondaryHost = "localhost";
            }
            int secondaryPort = 1245;

            int regPort = 1240;
            int expPort = 2240;

            String envRegPort = System.getenv("MANAGER_PORT");
            if (envRegPort != null && !envRegPort.trim().isEmpty()) {
                regPort = Integer.parseInt(envRegPort.trim());
                expPort = regPort + 1000;
            }

            try {
                LocateRegistry.createRegistry(regPort);
            } catch (Exception e) {
                // Registry may already exist
            }

            ReservationServerManager manager = new ReservationServerManager(
                    primaryHost, primaryPort, secondaryHost, secondaryPort, regPort, expPort);

            String managerUrl = "rmi://localhost:" + regPort + "/ReservationManager";
            String serviceUrl = "rmi://localhost:" + regPort + "/ReservationService";

            Naming.rebind(managerUrl, manager);
            Naming.rebind(serviceUrl, manager);

            System.out.println("=================================================");
            System.out.println("   RESERVATION SERVER MANAGER (ROUTER / PROXY)");
            System.out.println("=================================================");
            System.out.println("Registry Port: " + regPort);
            System.out.println("Export Port: " + expPort);
            System.out.println("Bound Manager URL: " + managerUrl);
            System.out.println("Bound Service URL: " + serviceUrl);
            System.out.println("Primary Replica Target: " + manager.getCurrentPrimaryUrl());
            System.out.println("Secondary Replica Target: " + manager.getSecondaryReplUrl());

            try {
                manager.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Manager ready and routing client requests to Primary...");
            System.out.println("=================================================");

        } catch (Exception e) {
            System.out.println("Manager Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
