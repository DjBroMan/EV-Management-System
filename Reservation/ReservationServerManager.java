import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.CristianClient;
import Clock.DistributedLogger;
import Clock.LamportResult;

// Reservation Server Manager coordinates primary-backup replication,
// full state synchronization, health checks, and failover promotion.
public class ReservationServerManager extends UnicastRemoteObject
        implements ReservationManagerInterface {

    private static final long serialVersionUID = 1L;
    private static final String SERVER_NAME = "ReservationServerManager";

    private final LogicalClock logicalClock = new LogicalClock();

    private String primaryUrl;
    private String secondaryUrl;
    private final int registryPort;

    public ReservationServerManager(
            String primaryUrl,
            String secondaryUrl,
            int registryPort,
            int exportPort) throws RemoteException {

        super(exportPort);
        this.primaryUrl = primaryUrl;
        this.secondaryUrl = secondaryUrl;
        this.registryPort = registryPort;
    }

    private void log(String message) {
        DistributedLogger.log(SERVER_NAME, logicalClock, message);
    }

    private void log(String eventType, String message) {
        DistributedLogger.log(SERVER_NAME, logicalClock, eventType, message);
    }

    public void setPrimaryUrl(String primaryUrl) {
        this.primaryUrl = primaryUrl;
    }

    public void setSecondaryUrl(String secondaryUrl) {
        this.secondaryUrl = secondaryUrl;
    }

    private ReservationReplicationInterface lookupSecondary() {
        try {
            return (ReservationReplicationInterface) Naming.lookup(secondaryUrl);
        } catch (Exception e) {
            log("LOCAL", "Warning: Secondary server at " + secondaryUrl + " is unreachable: " + e.getMessage());
            return null;
        }
    }

    private ReservationReplicationInterface lookupPrimary() {
        try {
            return (ReservationReplicationInterface) Naming.lookup(primaryUrl);
        } catch (Exception e) {
            log("LOCAL", "Warning: Primary server at " + primaryUrl + " is unreachable: " + e.getMessage());
            return null;
        }
    }

    public String synchronizeClock() {
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

        ReservationReplicationInterface secondary = lookupSecondary();
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

        ReservationReplicationInterface secondary = lookupSecondary();
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

        ReservationReplicationInterface primary = lookupPrimary();
        ReservationReplicationInterface secondary = lookupSecondary();

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

    @Override
    public LamportResult<Boolean> checkAndFailover(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "FAILOVER_CHECK received (Lamport: " + clientLamport + "). Clock updated to " + recvL);

        boolean primaryAlive = false;
        try {
            ReservationReplicationInterface primary = lookupPrimary();
            if (primary != null) {
                long pingSendL = logicalClock.sendEvent();
                LamportResult<Boolean> pingRes = primary.ping(pingSendL);
                logicalClock.receiveEvent(pingRes.getTimestamp());
                primaryAlive = pingRes.getData();
            }
        } catch (Exception e) {
            primaryAlive = false;
        }

        if (primaryAlive) {
            log("LOCAL", "Primary is healthy. No failover required.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }

        log("LOCAL", "PRIMARY SERVER FAILURE DETECTED! Initiating failover promotion to Secondary...");

        ReservationReplicationInterface secondary = lookupSecondary();
        if (secondary == null) {
            log("LOCAL", "FAILOVER_FAILED: Secondary is also unreachable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }

        try {
            long promoSendL = logicalClock.sendEvent();
            log("SEND", "Sending promotion command to Secondary (Lamport: " + promoSendL + ")");
            LamportResult<Boolean> promoRes = secondary.promoteToPrimary(promoSendL);
            logicalClock.receiveEvent(promoRes.getTimestamp());
            log("RECEIVE", "Secondary confirmed promotion to PRIMARY (Lamport: " + promoRes.getTimestamp() + ")");

            // Update Primary URL to point to newly promoted server
            this.primaryUrl = this.secondaryUrl;

            long respL = logicalClock.sendEvent();
            log("SEND", "FAILOVER_COMPLETED: Secondary successfully promoted to PRIMARY role (Lamport: " + respL + ")");
            return new LamportResult<>(true, respL);
        } catch (Exception e) {
            log("LOCAL", "FAILOVER_ERROR: Failed to promote Secondary: " + e.getMessage());
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(false, respL);
        }
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
            ReservationReplicationInterface primary = lookupPrimary();
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
            ReservationReplicationInterface secondary = lookupSecondary();
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

        String status = "=== REPLICATION CLUSTER STATUS ===\n"
                + "Primary Server   (" + primaryUrl + "): Status=" + (priAlive ? "ONLINE" : "OFFLINE")
                + ", Role=" + priRole + ", Active Reservations=" + priCount + "\n"
                + "Secondary Server (" + secondaryUrl + "): Status=" + (secAlive ? "ONLINE" : "OFFLINE")
                + ", Role=" + secRole + ", Replicated Reservations=" + secCount + "\n"
                + "Replication State: " + ((priAlive && secAlive && priCount == secCount) ? "SYNCHRONIZED" : "ATTENTION_REQUIRED");

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
            String primaryUrl = "rmi://" + primaryHost + ":1235/ReservationReplicationService";

            String secondaryHost = System.getenv("SECONDARY_HOST");
            if (secondaryHost == null || secondaryHost.trim().isEmpty()) {
                secondaryHost = "localhost";
            }
            String secondaryUrl = "rmi://" + secondaryHost + ":1245/ReservationReplicationService";

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

            ReservationServerManager manager = new ReservationServerManager(primaryUrl, secondaryUrl, regPort, expPort);

            String managerUrl = "rmi://localhost:" + regPort + "/ReservationManager";
            Naming.rebind(managerUrl, manager);

            System.out.println("=================================================");
            System.out.println("   RESERVATION SERVER MANAGER");
            System.out.println("=================================================");
            System.out.println("Registry Port: " + regPort);
            System.out.println("Export Port: " + expPort);
            System.out.println("Bound URL: " + managerUrl);
            System.out.println("Primary Replica Target: " + primaryUrl);
            System.out.println("Secondary Replica Target: " + secondaryUrl);

            try {
                manager.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Manager ready and actively coordinating replication...");
            System.out.println("=================================================");

        } catch (Exception e) {
            System.out.println("Manager Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
