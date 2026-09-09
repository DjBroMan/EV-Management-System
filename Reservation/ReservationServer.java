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

// RMI Server class that implements both ReservationInterface and ReservationReplicationInterface
// Supports both PRIMARY and SECONDARY roles using the same implementation.
public class ReservationServer extends UnicastRemoteObject
        implements ReservationInterface, ReservationReplicationInterface {

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

    // In-memory state: reservation ID -> reservation details string
    private final Map<String, String> reservations = new HashMap<>();

    // In-memory state: reservation ID -> charging port ID
    private final Map<String, String> reservationPorts = new HashMap<>();

    // Sequential counter for reservation IDs
    private int reservationCounter = 1001;

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

    private void ensureChargingStationConnected() {
        if (this.chargingStation != null)
            return;

        String stationUrl = System.getenv("STATION_URL");
        if (stationUrl == null || stationUrl.trim().isEmpty()) {
            String stationHost = System.getenv("STATION_HOST");
            if (stationHost == null || stationHost.trim().isEmpty()) {
                stationHost = "localhost";
            }
            stationUrl = "rmi://" + stationHost + ":1234//ChargingStationServer";
        }

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
                String stationUrl = System.getenv("STATION_URL");
                if (stationUrl == null || stationUrl.trim().isEmpty()) {
                    String stationHost = System.getenv("STATION_HOST");
                    if (stationHost == null || stationHost.trim().isEmpty()) {
                        stationHost = "localhost";
                    }
                    stationUrl = "rmi://" + stationHost + ":1234//ChargingStationServer";
                }

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

            String serviceBindUrl = "rmi://localhost:" + regPort + "/ReservationService";
            String replBindUrl = "rmi://localhost:" + regPort + "/ReservationReplicationService";

            Naming.rebind(serviceBindUrl, server);
            Naming.rebind(replBindUrl, server);

            System.out.println("=================================================");
            System.out.println("   RESERVATION RMI SERVER [" + role + "]");
            System.out.println("=================================================");
            System.out.println("Role: " + role);
            System.out.println("Registry Port: " + regPort);
            System.out.println("Export Port: " + expPort);
            System.out.println("Bound Service: " + serviceBindUrl);
            System.out.println("Bound Replication: " + replBindUrl);

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