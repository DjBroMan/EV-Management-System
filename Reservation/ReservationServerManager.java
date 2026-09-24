import java.rmi.Naming;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.CristianClient;
import Clock.DistributedLogger;
import Clock.LamportResult;
import Common.PeerHandle;
import Common.ClusterNodeInterface;
import Common.ClusterManagerInterface;
import Common.StateDelta;

/**
 * Reservation Server Manager coordinates primary-backup replication,
 * full state synchronization, health checks, failover promotion,
 * and acts as the SINGLE ENTRY POINT (Proxy / Router) for EVClient --
 * for ALL 5 clusters, not just Reservation:
 *
 * EVClient -> Manager (:1240) -> current leader/healthy instance of
 *             ReservationService / ChargingStationService /
 *             ChargingSessionService / PricingService / PaymentService
 *
 * The original Reservation-only proxy/replication/failover logic below
 * (reserveSlot/cancelReservation/getReservation/getReservationPort,
 * replicateReservation/replicateCancellation, triggerFullSynchronization,
 * checkAndFailover, getReplicationStatus, and both constructors) is
 * UNCHANGED from the original 2-node implementation so every existing
 * ReplicationTest scenario keeps passing exactly as before. Everything
 * below the "GENERALIZED MULTI-CLUSTER ROUTING" marker is new: it adds
 * ChargingStation/ChargingSession/Pricing/Payment proxy routing, N-instance
 * health monitoring + round-robin load balancing for reads, leader-only
 * routing for writes, and the ClusterManagerInterface replication fan-out
 * used by those 4 clusters' Bully-elected PRIMARYs.
 */
public class ReservationServerManager extends UnicastRemoteObject
        implements ReservationManagerInterface, ChargingStationInterface, ChargingSessionInterface,
                   PricingInterface, PaymentInterface, ClusterManagerInterface {

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

        initGeneralizedClusters();
    }

    public ReservationServerManager(
            String primaryUrl,
            String secondaryUrl,
            int registryPort,
            int exportPort) throws RemoteException {

        super(exportPort);
        this.registryPort = registryPort;
        parseAndSetEndpoints(primaryUrl, secondaryUrl);

        initGeneralizedClusters();
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

        if (reservationClusterMode) {
            return proxyWrite("Reservation", (ReservationInterface s) -> s.reserveSlot(userId, vehicleId, clientLamport),
                    "Reservation failed: Reservation cluster is unavailable.");
        }

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

        if (reservationClusterMode) {
            return proxyWrite("Reservation", (ReservationInterface s) -> s.cancelReservation(reservationId, clientLamport),
                    "Cancellation failed: Reservation cluster is unavailable.");
        }

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

        if (reservationClusterMode) {
            return proxyRead("Reservation", (ReservationInterface s) -> s.getReservation(reservationId, clientLamport),
                    "Reservation " + reservationId + " not found (cluster unreachable).");
        }

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

        if (reservationClusterMode) {
            return proxyRead("Reservation", (ReservationInterface s) -> s.getReservationPort(reservationId, clientLamport), "NONE");
        }

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

        if (reservationClusterMode) {
            // N-instance mode: fan out to every Reservation cluster instance via the
            // generic ClusterNodeInterface path instead of the stale fixed-secondary
            // assumption (the actual leader/secondary set is whatever Bully currently
            // says it is, tracked by the health monitor, not a hardcoded port pair).
            boolean anyOk = fanOutReservationCluster(reservationId, reservationDetails, portId, currentCounter);
            long respL2 = logicalClock.sendEvent();
            log("SEND", "Returning REPLICATION_CONFIRMED to Primary (Lamport: " + respL2 + ")");
            return new LamportResult<>(anyOk, respL2);
        }

        boolean success;
        // If current primary is already the secondary, replication is not forwarded back to itself
        if (this.currentPrimaryPort == this.secondaryPort) {
            success = true;
        } else {
            ReservationReplicationInterface secondary = lookupSecondaryReplication();
            if (secondary == null) {
                log("LOCAL", "REPLICATION_FAILED: Secondary replica is unreachable. Replication aborted.");
                success = false;
            } else {
                long sendL = logicalClock.sendEvent();
                log("SEND", "REPLICATION_SENT: Forwarding state update for " + reservationId + " -> " + portId + " to Secondary (Lamport: " + sendL + ")");
                try {
                    LamportResult<Boolean> secRes = secondary.applyReservationUpdate(reservationId, reservationDetails, portId, currentCounter, sendL);
                    logicalClock.receiveEvent(secRes.getTimestamp());
                    log("RECEIVE", "Secondary acknowledged reservation replication for " + reservationId + " (Secondary Lamport: " + secRes.getTimestamp() + ")");
                    success = secRes.getData();
                } catch (Exception e) {
                    log("LOCAL", "REPLICATION_ERROR: Failed to replicate to Secondary: " + e.getMessage());
                    success = false;
                }
            }
        }

        // Additional generalized fan-out: any Reservation instances beyond the
        // classic primary/secondary pair (e.g. a 3rd node R3) configured via
        // RESERVATION_INSTANCES also receive this update, best-effort.
        fanOutReservationExtras(reservationId, reservationDetails, portId, currentCounter);

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning REPLICATION_CONFIRMED to Primary (Lamport: " + respL + ")");
        return new LamportResult<>(success, respL);
    }

    @Override
    public LamportResult<Boolean> replicateCancellation(
            String reservationId,
            long clientLamport) throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "CANCELLATION_UPDATE received from Primary for " + reservationId + " (Primary Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (reservationClusterMode) {
            boolean anyOk = fanOutReservationClusterCancel(reservationId);
            long respL2 = logicalClock.sendEvent();
            log("SEND", "Returning CANCELLATION_CONFIRMED to Primary (Lamport: " + respL2 + ")");
            return new LamportResult<>(anyOk, respL2);
        }

        boolean success;
        if (this.currentPrimaryPort == this.secondaryPort) {
            success = true;
        } else {
            ReservationReplicationInterface secondary = lookupSecondaryReplication();
            if (secondary == null) {
                log("LOCAL", "CANCELLATION_REPLICATION_FAILED: Secondary replica is unreachable.");
                success = false;
            } else {
                long sendL = logicalClock.sendEvent();
                log("SEND", "REPLICATION_SENT: Forwarding cancellation update for " + reservationId + " to Secondary (Lamport: " + sendL + ")");
                try {
                    LamportResult<Boolean> secRes = secondary.applyCancellationUpdate(reservationId, sendL);
                    logicalClock.receiveEvent(secRes.getTimestamp());
                    log("RECEIVE", "Secondary acknowledged cancellation replication for " + reservationId + " (Secondary Lamport: " + secRes.getTimestamp() + ")");
                    success = secRes.getData();
                } catch (Exception e) {
                    log("LOCAL", "CANCELLATION_REPLICATION_ERROR: Failed to replicate cancellation: " + e.getMessage());
                    success = false;
                }
            }
        }

        fanOutReservationCancelExtras(reservationId);

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning CANCELLATION_CONFIRMED to Primary (Lamport: " + respL + ")");
        return new LamportResult<>(success, respL);
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
                + "Cluster Synchronization: " + ((priAlive && secAlive && priCount == secCount) ? "SYNCHRONIZED" : "ATTENTION_REQUIRED")
                + "\n\n" + describeGeneralizedClusters();

        long respL = logicalClock.sendEvent();
        return new LamportResult<>(status, respL);
    }

    // =========================================================================================
    // GENERALIZED MULTI-CLUSTER ROUTING (ChargingStation / ChargingSession / Pricing / Payment,
    // plus supplementary N-node fan-out for Reservation). Everything above this marker is the
    // original, unmodified 2-node Reservation Manager logic.
    // =========================================================================================

    /** Per-cluster runtime state: static instance list, round-robin cursor, discovered leader. */
    private static class ClusterConfig {
        final String bindPrefix; // e.g. "ChargingStationService" -- matches "<prefix>-<id>" bound names
        final List<PeerHandle> instances;
        final AtomicInteger roundRobinCursor = new AtomicInteger(0);
        volatile PeerHandle currentLeader;
        final Map<Integer, Boolean> healthy = new ConcurrentHashMap<>();

        ClusterConfig(String bindPrefix, List<PeerHandle> instances) {
            this.bindPrefix = bindPrefix;
            this.instances = instances;
            for (PeerHandle p : instances) healthy.put(p.id, true);
        }
    }

    private final Map<String, ClusterConfig> clusters = new ConcurrentHashMap<>();
    private List<PeerHandle> reservationExtraInstances = new ArrayList<>(); // any Reservation nodes beyond primary/secondary
    private final ScheduledExecutorService healthMonitor = Executors.newScheduledThreadPool(1);

    private static List<PeerHandle> parseInstances(String envVar, int defaultId, String defaultHost, int defaultPort) {
        List<PeerHandle> result = new ArrayList<>();
        String raw = System.getenv(envVar);
        if (raw != null && !raw.trim().isEmpty()) {
            for (String entry : raw.split(",")) {
                entry = entry.trim();
                if (entry.isEmpty()) continue;
                String[] parts = entry.split(":");
                if (parts.length != 3) continue;
                try {
                    result.add(new PeerHandle(Integer.parseInt(parts[0].trim()), parts[1].trim(), Integer.parseInt(parts[2].trim())));
                } catch (NumberFormatException ignored) { }
            }
        }
        if (result.isEmpty()) {
            result.add(new PeerHandle(defaultId, defaultHost, defaultPort));
        }
        return result;
    }

    // When set, Reservation routing uses the SAME generalized N-instance
    // Bully-leader-discovery + round-robin path as the other 4 clusters
    // instead of the legacy hardcoded 2-node currentPrimaryHost/Port swap.
    // Left false by default (RESERVATION_INSTANCES unset) so every existing
    // ReplicationTest scenario keeps exercising the exact original,
    // already-verified 2-node failover code path unchanged. Docker/the
    // 3-node production deployment sets RESERVATION_INSTANCES to opt in --
    // running both mechanisms live at once against the same nodes would risk
    // the Manager's reactive 2-node swap and the cluster's own Bully election
    // disagreeing about who is Primary (split-brain), so exactly one drives
    // real routing at a time.
    private boolean reservationClusterMode = false;

    private void initGeneralizedClusters() {
        String resInstancesEnv = System.getenv("RESERVATION_INSTANCES");
        reservationClusterMode = resInstancesEnv != null && !resInstancesEnv.trim().isEmpty();
        clusters.put("Reservation", new ClusterConfig("Reservation",
                parseInstances("RESERVATION_INSTANCES", 1, primaryHost, primaryPort)));

        clusters.put("ChargingStation", new ClusterConfig("ChargingStationService",
                parseInstances("STATION_INSTANCES", 1, "localhost", 1234)));
        clusters.put("ChargingSession", new ClusterConfig("ChargingSessionService",
                parseInstances("SESSION_INSTANCES", 1, "localhost", 1236)));
        clusters.put("Pricing", new ClusterConfig("PricingService",
                parseInstances("PRICING_INSTANCES", 1, "localhost", 1238)));
        clusters.put("Payment", new ClusterConfig("PaymentService",
                parseInstances("PAYMENT_INSTANCES", 1, "localhost", 1237)));

        String extrasRaw = System.getenv("RESERVATION_EXTRA_INSTANCES");
        if (extrasRaw != null && !extrasRaw.trim().isEmpty()) {
            for (String entry : extrasRaw.split(",")) {
                entry = entry.trim();
                if (entry.isEmpty()) continue;
                String[] parts = entry.split(":");
                if (parts.length != 3) continue;
                try {
                    reservationExtraInstances.add(new PeerHandle(Integer.parseInt(parts[0].trim()), parts[1].trim(), Integer.parseInt(parts[2].trim())));
                } catch (NumberFormatException ignored) { }
            }
        }

        healthMonitor.scheduleWithFixedDelay(this::healthAndLeaderDiscoveryTick, 5, 4, TimeUnit.SECONDS);
    }

    @SuppressWarnings("unchecked")
    private <S extends Remote> S lookupTyped(String cluster, PeerHandle instance) throws Exception {
        ClusterConfig cfg = clusters.get(cluster);
        String url = "rmi://" + instance.host + ":" + instance.registryPort + "/" + cfg.bindPrefix + "-" + instance.id;
        return (S) Naming.lookup(url);
    }

    private void healthAndLeaderDiscoveryTick() {
        for (Map.Entry<String, ClusterConfig> entry : clusters.entrySet()) {
            String clusterName = entry.getKey();
            ClusterConfig cfg = entry.getValue();
            PeerHandle discoveredLeader = null;
            for (PeerHandle instance : cfg.instances) {
                try {
                    ClusterNodeInterface node = lookupTyped(clusterName, instance);
                    long sendL = logicalClock.sendEvent();
                    node.ping(sendL);
                    cfg.healthy.put(instance.id, true);

                    long rL = logicalClock.sendEvent();
                    LamportResult<String> roleRes = node.getRole(rL);
                    if ("PRIMARY".equals(roleRes.getData())) {
                        discoveredLeader = instance;
                    }
                } catch (Exception e) {
                    cfg.healthy.put(instance.id, false);
                }
            }
            if (discoveredLeader != null && !discoveredLeader.equals(cfg.currentLeader)) {
                log("MANAGER", "New leader for " + clusterName + " cluster = " + cfg.bindPrefix + "-" + discoveredLeader.id);
                cfg.currentLeader = discoveredLeader;
            } else if (discoveredLeader != null) {
                cfg.currentLeader = discoveredLeader;
            }
        }
    }

    private String describeGeneralizedClusters() {
        StringBuilder sb = new StringBuilder("=== GENERALIZED CLUSTER STATUS (Manager view) ===\n");
        for (Map.Entry<String, ClusterConfig> entry : clusters.entrySet()) {
            ClusterConfig cfg = entry.getValue();
            sb.append(entry.getKey()).append(": instances=").append(cfg.instances)
              .append(", healthy=").append(cfg.healthy)
              .append(", leader=").append(cfg.currentLeader == null ? "UNKNOWN" : cfg.bindPrefix + "-" + cfg.currentLeader.id)
              .append("\n");
        }
        return sb.toString();
    }

    private PeerHandle pickForRead(String cluster) {
        ClusterConfig cfg = clusters.get(cluster);
        List<PeerHandle> healthyInstances = new ArrayList<>();
        for (PeerHandle p : cfg.instances) {
            if (Boolean.TRUE.equals(cfg.healthy.getOrDefault(p.id, true))) {
                healthyInstances.add(p);
            }
        }
        if (healthyInstances.isEmpty()) {
            healthyInstances = cfg.instances; // fall back to trying anyway
        }
        int idx = Math.floorMod(cfg.roundRobinCursor.getAndIncrement(), healthyInstances.size());
        PeerHandle chosen = healthyInstances.get(idx);
        log("LOAD BALANCER", "Selected " + cfg.bindPrefix + "-" + chosen.id + " (read, round-robin)");
        return chosen;
    }

    private PeerHandle pickForWrite(String cluster) {
        ClusterConfig cfg = clusters.get(cluster);
        if (cfg.currentLeader != null) {
            log("LOAD BALANCER", "Write operation -> routing to leader " + cfg.bindPrefix + "-" + cfg.currentLeader.id + ", bypassing round-robin");
            return cfg.currentLeader;
        }
        // No leader discovered yet -- force an immediate discovery pass before giving up
        healthAndLeaderDiscoveryTick();
        return cfg.currentLeader;
    }

    // ---- ChargingStationInterface proxy (read-only methods load-balanced; writes leader-only) ----

    @Override
    public String getStationStatus() throws RemoteException { return getStationStatus(0).getData(); }

    @Override
    public LamportResult<String> getStationStatus(long clientLamport) throws RemoteException {
        return proxyRead("ChargingStation", (ChargingStationInterface s) -> s.getStationStatus(clientLamport),
                "Station status unavailable: cluster unreachable.");
    }

    @Override
    public String getAvailablePorts() throws RemoteException { return getAvailablePorts(0).getData(); }

    @Override
    public LamportResult<String> getAvailablePorts(long clientLamport) throws RemoteException {
        return proxyRead("ChargingStation", (ChargingStationInterface s) -> s.getAvailablePorts(clientLamport),
                "Available ports unavailable: cluster unreachable.");
    }

    @Override
    public String checkPortAvailability(String portId) throws RemoteException { return checkPortAvailability(portId, 0).getData(); }

    @Override
    public LamportResult<String> checkPortAvailability(String portId, long clientLamport) throws RemoteException {
        return proxyRead("ChargingStation", (ChargingStationInterface s) -> s.checkPortAvailability(portId, clientLamport),
                "Port check unavailable: cluster unreachable.");
    }

    @Override
    public String reservePort(String portId) throws RemoteException { return reservePort(portId, 0).getData(); }

    @Override
    public LamportResult<String> reservePort(String portId, long clientLamport) throws RemoteException {
        return proxyWrite("ChargingStation", (ChargingStationInterface s) -> s.reservePort(portId, clientLamport),
                "Reservation failed: ChargingStation cluster is unavailable.");
    }

    @Override
    public String releasePort(String portId) throws RemoteException { return releasePort(portId, 0).getData(); }

    @Override
    public LamportResult<String> releasePort(String portId, long clientLamport) throws RemoteException {
        return proxyWrite("ChargingStation", (ChargingStationInterface s) -> s.releasePort(portId, clientLamport),
                "Release failed: ChargingStation cluster is unavailable.");
    }

    @Override
    public String reserveAnyAvailablePort() throws RemoteException { return reserveAnyAvailablePort(0).getData(); }

    @Override
    public LamportResult<String> reserveAnyAvailablePort(long clientLamport) throws RemoteException {
        return proxyWrite("ChargingStation", (ChargingStationInterface s) -> s.reserveAnyAvailablePort(clientLamport),
                "NONE");
    }

    @Override
    public String startPortCharging(String portId) throws RemoteException { return startPortCharging(portId, 0).getData(); }

    @Override
    public LamportResult<String> startPortCharging(String portId, long clientLamport) throws RemoteException {
        return proxyWrite("ChargingStation", (ChargingStationInterface s) -> s.startPortCharging(portId, clientLamport),
                "PORT_NOT_FOUND");
    }

    // ---- ChargingSessionInterface proxy ----

    @Override
    public String startCharging(String reservationId) throws RemoteException { return startCharging(reservationId, 0).getData(); }

    @Override
    public LamportResult<String> startCharging(String reservationId, long clientLamport) throws RemoteException {
        return proxyWrite("ChargingSession", (ChargingSessionInterface s) -> s.startCharging(reservationId, clientLamport),
                "Start charging failed: ChargingSession cluster is unavailable.");
    }

    @Override
    public String stopCharging(String sessionId) throws RemoteException { return stopCharging(sessionId, 0).getData(); }

    @Override
    public LamportResult<String> stopCharging(String sessionId, long clientLamport) throws RemoteException {
        return proxyWrite("ChargingSession", (ChargingSessionInterface s) -> s.stopCharging(sessionId, clientLamport),
                "Stop charging failed: ChargingSession cluster is unavailable.");
    }

    @Override
    public String getSessionStatus(String sessionId) throws RemoteException { return getSessionStatus(sessionId, 0).getData(); }

    @Override
    public LamportResult<String> getSessionStatus(String sessionId, long clientLamport) throws RemoteException {
        return proxyRead("ChargingSession", (ChargingSessionInterface s) -> s.getSessionStatus(sessionId, clientLamport),
                "Session status unavailable: cluster unreachable.");
    }

    @Override
    public double getEnergyConsumed(String sessionId) throws RemoteException { return getEnergyConsumed(sessionId, 0).getData(); }

    @Override
    public LamportResult<Double> getEnergyConsumed(String sessionId, long clientLamport) throws RemoteException {
        return proxyReadDouble("ChargingSession", (ChargingSessionInterface s) -> s.getEnergyConsumed(sessionId, clientLamport), -1.0);
    }

    @Override
    public String getSessionPort(String sessionId) throws RemoteException { return getSessionPort(sessionId, 0).getData(); }

    @Override
    public LamportResult<String> getSessionPort(String sessionId, long clientLamport) throws RemoteException {
        return proxyRead("ChargingSession", (ChargingSessionInterface s) -> s.getSessionPort(sessionId, clientLamport), "NONE");
    }

    // ---- PricingInterface proxy (no leader concept -- every instance is a read replica) ----

    @Override
    public double calculatePrice(String stationId, double energyConsumed) throws RemoteException { return calculatePrice(stationId, energyConsumed, 0).getData(); }

    @Override
    public LamportResult<Double> calculatePrice(String stationId, double energyConsumed, long clientLamport) throws RemoteException {
        return proxyReadDouble("Pricing", (PricingInterface s) -> s.calculatePrice(stationId, energyConsumed, clientLamport), -1.0);
    }

    @Override
    public double getDemandMultiplier(String stationId) throws RemoteException { return getDemandMultiplier(stationId, 0).getData(); }

    @Override
    public LamportResult<Double> getDemandMultiplier(String stationId, long clientLamport) throws RemoteException {
        return proxyReadDouble("Pricing", (PricingInterface s) -> s.getDemandMultiplier(stationId, clientLamport), 1.0);
    }

    // ---- PaymentInterface proxy ----

    @Override
    public String makePayment(String sessionId) throws RemoteException { return makePayment(sessionId, 0).getData(); }

    @Override
    public LamportResult<String> makePayment(String sessionId, long clientLamport) throws RemoteException {
        return proxyWrite("Payment", (PaymentInterface s) -> s.makePayment(sessionId, clientLamport),
                "Payment failed: Payment cluster is unavailable.");
    }

    @Override
    public String getPaymentStatus(String paymentId) throws RemoteException { return getPaymentStatus(paymentId, 0).getData(); }

    @Override
    public LamportResult<String> getPaymentStatus(String paymentId, long clientLamport) throws RemoteException {
        return proxyRead("Payment", (PaymentInterface s) -> s.getPaymentStatus(paymentId, clientLamport), "Payment not found.");
    }

    @Override
    public String getPaymentDetails(String paymentId) throws RemoteException { return getPaymentDetails(paymentId, 0).getData(); }

    @Override
    public LamportResult<String> getPaymentDetails(String paymentId, long clientLamport) throws RemoteException {
        return proxyRead("Payment", (PaymentInterface s) -> s.getPaymentDetails(paymentId, clientLamport), "Payment not found.");
    }

    // ---- Wallet (PaymentInterface) proxy ----

    @Override
    public String addFunds(String userId, double amount) throws RemoteException { return addFunds(userId, amount, 0).getData(); }

    @Override
    public LamportResult<String> addFunds(String userId, double amount, long clientLamport) throws RemoteException {
        return proxyWrite("Payment", (PaymentInterface s) -> s.addFunds(userId, amount, clientLamport),
                "Add funds failed: Payment cluster is unavailable.");
    }

    @Override
    public double getWalletBalance(String userId) throws RemoteException { return getWalletBalance(userId, 0).getData(); }

    @Override
    public LamportResult<Double> getWalletBalance(String userId, long clientLamport) throws RemoteException {
        return proxyReadDouble("Payment", (PaymentInterface s) -> s.getWalletBalance(userId, clientLamport), -1.0);
    }

    @Override
    public String checkAndDeductBalance(String userId, double amount, String sessionId) throws RemoteException {
        return checkAndDeductBalance(userId, amount, sessionId, 0).getData();
    }

    @Override
    public LamportResult<String> checkAndDeductBalance(String userId, double amount, String sessionId, long clientLamport) throws RemoteException {
        return proxyWrite("Payment", (PaymentInterface s) -> s.checkAndDeductBalance(userId, amount, sessionId, clientLamport),
                "INSUFFICIENT|0.0");
    }

    // ---- Generic proxy helpers ----

    @FunctionalInterface private interface StringCall<S> { LamportResult<String> call(S stub) throws RemoteException; }
    @FunctionalInterface private interface DoubleCall<S> { LamportResult<Double> call(S stub) throws RemoteException; }

    private <S> LamportResult<String> proxyRead(String cluster, StringCall<S> call, String fallback) throws RemoteException {
        PeerHandle target = pickForRead(cluster);
        try {
            @SuppressWarnings("unchecked")
            S stub = (S) lookupTyped(cluster, target);
            return call.call(stub);
        } catch (Exception e) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(fallback + " (" + e.getMessage() + ")", respL);
        }
    }

    private <S> LamportResult<Double> proxyReadDouble(String cluster, DoubleCall<S> call, double fallback) throws RemoteException {
        PeerHandle target = pickForRead(cluster);
        try {
            @SuppressWarnings("unchecked")
            S stub = (S) lookupTyped(cluster, target);
            return call.call(stub);
        } catch (Exception e) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(fallback, respL);
        }
    }

    private <S> LamportResult<String> proxyWrite(String cluster, StringCall<S> call, String fallback) throws RemoteException {
        PeerHandle target = pickForWrite(cluster);
        if (target == null) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(fallback, respL);
        }
        try {
            @SuppressWarnings("unchecked")
            S stub = (S) lookupTyped(cluster, target);
            return call.call(stub);
        } catch (Exception e) {
            log("LOCAL", "Write to leader " + target + " on cluster " + cluster + " failed: " + e.getMessage() + ". Forcing re-discovery and retrying once.");
            healthAndLeaderDiscoveryTick();
            PeerHandle retryTarget = clusters.get(cluster).currentLeader;
            if (retryTarget != null && !retryTarget.equals(target)) {
                try {
                    @SuppressWarnings("unchecked")
                    S stub2 = (S) lookupTyped(cluster, retryTarget);
                    return call.call(stub2);
                } catch (Exception e2) {
                    // fall through to fallback response below
                }
            }
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(fallback + " (" + e.getMessage() + ")", respL);
        }
    }

    // ---- ClusterManagerInterface: replication fan-out for ChargingStation/ChargingSession/Payment ----

    @Override
    public LamportResult<Boolean> replicateUpdate(String serviceName, int originServerId, StateDelta delta, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        String cluster = mapServiceNameToCluster(serviceName);
        ClusterConfig cfg = clusters.get(cluster);
        if (cfg == null) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }
        log("REPLICATION", "Fan-out " + delta + " to " + (cfg.instances.size() - 1) + " peer " + cluster
                + " instances (excluding origin " + cfg.bindPrefix + "-" + originServerId + ", already applied there)");
        for (PeerHandle instance : cfg.instances) {
            if (instance.id == originServerId) {
                continue; // the origin already applied this change locally -- fanning out to
                          // it too would call back into its own still-held synchronized write
                          // method (self-deadlock) and is redundant anyway.
            }
            try {
                ClusterNodeInterface node = lookupTyped(cluster, instance);
                long sendL = logicalClock.sendEvent();
                node.applyUpdate(delta, sendL);
                log("REPLICATION", "Update sent to " + cfg.bindPrefix + "-" + instance.id);
            } catch (Exception e) {
                log("LOCAL", "WARNING: fan-out to " + cfg.bindPrefix + "-" + instance.id + " failed: " + e.getMessage());
            }
        }
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    private String mapServiceNameToCluster(String serviceName) {
        if (serviceName == null) return "";
        if (serviceName.startsWith("ChargingStation")) return "ChargingStation";
        if (serviceName.startsWith("ChargingSession")) return "ChargingSession";
        if (serviceName.startsWith("Pricing")) return "Pricing";
        if (serviceName.startsWith("Payment")) return "Payment";
        return serviceName;
    }

    private void fanOutReservationExtras(String reservationId, String details, String portId, int counter) {
        for (PeerHandle extra : reservationExtraInstances) {
            try {
                String url = "rmi://" + extra.host + ":" + extra.registryPort + "/Reservation-" + extra.id;
                ClusterNodeInterface node = (ClusterNodeInterface) Naming.lookup(url);
                long sendL = logicalClock.sendEvent();
                node.applyUpdate(new StateDelta("RESERVATION_UPSERT", reservationId, details, portId, counter), sendL);
                log("REPLICATION", "Update sent to Reservation-" + extra.id + " (extra cluster node)");
            } catch (Exception e) {
                log("LOCAL", "WARNING: extra Reservation fan-out to " + extra + " failed: " + e.getMessage());
            }
        }
    }

    private void fanOutReservationCancelExtras(String reservationId) {
        for (PeerHandle extra : reservationExtraInstances) {
            try {
                String url = "rmi://" + extra.host + ":" + extra.registryPort + "/Reservation-" + extra.id;
                ClusterNodeInterface node = (ClusterNodeInterface) Naming.lookup(url);
                long sendL = logicalClock.sendEvent();
                node.applyUpdate(new StateDelta("RESERVATION_DELETE", reservationId), sendL);
            } catch (Exception e) {
                log("LOCAL", "WARNING: extra Reservation cancel fan-out to " + extra + " failed: " + e.getMessage());
            }
        }
    }

    /** N-instance-mode replication fan-out: sends to every configured Reservation instance. */
    private boolean fanOutReservationCluster(String reservationId, String details, String portId, int counter) {
        ClusterConfig cfg = clusters.get("Reservation");
        // In cluster mode, writes are always routed to whoever cfg.currentLeader
        // is (pickForWrite), so the leader IS the origin of this change by
        // construction -- skip it here exactly like the ChargingStation/
        // ChargingSession/Payment fan-out does, both because it already applied
        // the change locally (redundant) and because re-applying a raw INSERT
        // against its own DB throws a duplicate-key error (observed live: "[DB]
        // WARNING: Failed to persist reservation RES1002: Duplicate entry").
        int originId = (cfg.currentLeader != null) ? cfg.currentLeader.id : -1;
        boolean anyOk = false;
        for (PeerHandle instance : cfg.instances) {
            if (instance.id == originId) continue;
            try {
                ClusterNodeInterface node = lookupTyped("Reservation", instance);
                long sendL = logicalClock.sendEvent();
                node.applyUpdate(new StateDelta("RESERVATION_UPSERT", reservationId, details, portId, counter), sendL);
                log("REPLICATION", "Update sent to Reservation-" + instance.id);
                anyOk = true;
            } catch (Exception e) {
                log("LOCAL", "WARNING: cluster fan-out to Reservation-" + instance.id + " failed: " + e.getMessage());
            }
        }
        return anyOk;
    }

    private boolean fanOutReservationClusterCancel(String reservationId) {
        ClusterConfig cfg = clusters.get("Reservation");
        int originId = (cfg.currentLeader != null) ? cfg.currentLeader.id : -1;
        boolean anyOk = false;
        for (PeerHandle instance : cfg.instances) {
            if (instance.id == originId) continue;
            try {
                ClusterNodeInterface node = lookupTyped("Reservation", instance);
                long sendL = logicalClock.sendEvent();
                node.applyUpdate(new StateDelta("RESERVATION_DELETE", reservationId), sendL);
                log("REPLICATION", "Cancellation sent to Reservation-" + instance.id);
                anyOk = true;
            } catch (Exception e) {
                log("LOCAL", "WARNING: cluster cancel fan-out to Reservation-" + instance.id + " failed: " + e.getMessage());
            }
        }
        return anyOk;
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
            String stationUrl = "rmi://localhost:" + regPort + "/ChargingStationService";
            String sessionUrl = "rmi://localhost:" + regPort + "/ChargingSessionService";
            String pricingUrl = "rmi://localhost:" + regPort + "/PricingService";
            String paymentUrl = "rmi://localhost:" + regPort + "/PaymentService";
            String clusterMgrUrl = "rmi://localhost:" + regPort + "/ClusterManager";

            Naming.rebind(managerUrl, manager);
            Naming.rebind(serviceUrl, manager);
            Naming.rebind(stationUrl, manager);
            Naming.rebind(sessionUrl, manager);
            Naming.rebind(pricingUrl, manager);
            Naming.rebind(paymentUrl, manager);
            Naming.rebind(clusterMgrUrl, manager);

            System.out.println("=================================================");
            System.out.println("   EV MANAGER (SINGLE ENTRY POINT / ROUTER / PROXY)");
            System.out.println("=================================================");
            System.out.println("Registry Port: " + regPort);
            System.out.println("Export Port: " + expPort);
            System.out.println("Bound: " + managerUrl);
            System.out.println("Bound: " + serviceUrl);
            System.out.println("Bound: " + stationUrl);
            System.out.println("Bound: " + sessionUrl);
            System.out.println("Bound: " + pricingUrl);
            System.out.println("Bound: " + paymentUrl);
            System.out.println("Bound: " + clusterMgrUrl);
            System.out.println("Reservation Primary Target: " + manager.getCurrentPrimaryUrl());
            System.out.println("Reservation Secondary Target: " + manager.getSecondaryReplUrl());
            System.out.println(manager.describeGeneralizedClusters());

            try {
                manager.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Manager ready and routing client requests for all 5 services...");
            System.out.println("=================================================");

        } catch (Exception e) {
            System.out.println("Manager Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
