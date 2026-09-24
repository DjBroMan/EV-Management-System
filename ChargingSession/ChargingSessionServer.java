import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.util.HashMap;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

// RMI Server class implementing ChargingSessionInterface. Runs as one of N
// instances in the ChargingSession cluster (S1/S2/S3). PRIMARY accepts
// startCharging/stopCharging; SECONDARY replicas reject writes and serve
// load-balanced reads (getSessionStatus/getEnergyConsumed/getSessionPort).
public class ChargingSessionServer
        extends UnicastRemoteObject
        implements ChargingSessionInterface, ClusterNodeInterface {

    private static final double DEFAULT_CHARGING_POWER_KW = 7.2;
    private static final String STATION_ID = "S01";
    private static final long BILLING_INTERVAL_SECONDS = 5;

    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private final LogicalClock logicalClock = new LogicalClock();

    private HashMap<String, String> reservationSessions;
    private HashMap<String, String> sessionStatus;
    private HashMap<String, Double> energyConsumed;
    private HashMap<String, String> sessionPort;
    private HashMap<String, Instant> sessionStartTimes;
    private HashMap<String, Instant> sessionEndTimes;
    private HashMap<String, Double> sessionChargingPowers;

    // Wallet billing: which user pays for a session, and when that session's
    // energy consumption was last billed against their wallet.
    private HashMap<String, String> sessionUserIds;
    private HashMap<String, Instant> lastBillingCheckpoints;
    private ScheduledExecutorService billingScheduler;

    private int sessionCounter = 1001;

    private ReservationInterface reservationServer;
    private ChargingStationInterface chargingStation;
    private PaymentInterface payment;
    private PricingInterface pricing;

    private final ServerIdentity identity;
    private volatile String role;
    private BullyElection election;
    private ClusterManagerClient managerClient;

    private ChargingSessionDAO dao = null;

    public ChargingSessionServer(
            ServerIdentity identity,
            ReservationInterface reservationServer,
            ChargingStationInterface chargingStation,
            PaymentInterface payment,
            PricingInterface pricing)
            throws RemoteException {

        super(identity.exportPort);
        this.identity = identity;
        this.role = identity.role;

        reservationSessions = new HashMap<String, String>();
        sessionStatus = new HashMap<String, String>();
        energyConsumed = new HashMap<String, Double>();
        sessionPort = new HashMap<String, String>();

        sessionStartTimes = new HashMap<String, Instant>();
        sessionEndTimes = new HashMap<String, Instant>();
        sessionChargingPowers = new HashMap<String, Double>();

        sessionUserIds = new HashMap<String, String>();
        lastBillingCheckpoints = new HashMap<String, Instant>();

        this.reservationServer = reservationServer;
        this.chargingStation = chargingStation;
        this.payment = payment;
        this.pricing = pricing;
    }

    private String serverName() {
        return "ChargingSessionServer-" + identity.serverId + "[" + role + "]";
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
            this.dao = new ChargingSessionDAO();
            dao.loadAllSessions(reservationSessions, sessionStatus, energyConsumed,
                    sessionPort, sessionStartTimes, sessionEndTimes, sessionChargingPowers);
            int maxCounter = dao.getMaxCounter();
            if (maxCounter >= sessionCounter) {
                sessionCounter = maxCounter + 1;
            }
            System.out.println("[DB:" + serverName() + "] Database initialized. Counter set to " + sessionCounter);
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
                () -> { this.role = "PRIMARY"; log("BULLY", "This instance is now PRIMARY (coordinator) of the ChargingSession cluster."); },
                (leader) -> { this.role = "SECONDARY"; log("BULLY", "Learned new coordinator: ChargingSession-" + leader.id); });

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

    private String formatInstant(Instant instant) {
        if (instant == null) return "N/A";
        return TIME_FORMATTER.format(instant);
    }

    // Reservation.getReservation returns a free-text sentence, e.g.
    // "Reservation ID: R1001, User ID: USER1, Vehicle ID: EV1, Port: P1, Status: CONFIRMED"
    // -- extract the userId token the same way the rest of this codebase
    // parses these free-text responses (see PaymentServer.makePayment's
    // "COMPLETED" substring check).
    private String extractUserId(String reservationInfo) {
        if (reservationInfo == null) return null;
        int idx = reservationInfo.indexOf("User ID: ");
        if (idx == -1) return null;
        int start = idx + "User ID: ".length();
        int end = reservationInfo.indexOf(",", start);
        if (end == -1) end = reservationInfo.length();
        return reservationInfo.substring(start, end).trim();
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
    // START CHARGING (PRIMARY-only write)
    // =========================================================

    @Override
    public String startCharging(String reservationId) throws RemoteException {
        return startCharging(reservationId, 0).getData();
    }

    @Override
    public LamportResult<String> startCharging(
            String reservationId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "START CHARGING request received for Reservation: " + reservationId + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (rejectIfSecondary("startCharging")) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Start charging failed: this instance is SECONDARY. Route to PRIMARY.", respL);
        }

        simulateProcessing(400);

        if (reservationId == null || reservationId.length() == 0) {
            log("LOCAL", "Task failed: Invalid Reservation ID.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Invalid Reservation ID.", respL);
        }

        synchronized (this) {
            if (reservationSessions.containsKey(reservationId)) {
                String existingSession = reservationSessions.get(reservationId);
                log("LOCAL", "Existing session found: " + existingSession);
                long respL = logicalClock.sendEvent();
                return new LamportResult<>("Charging session already exists.\nSession ID: " + existingSession, respL);
            }
        }

        long sendL1 = logicalClock.sendEvent();
        log("SEND", "Contacting ReservationServer.getReservation for " + reservationId + " (Lamport: " + sendL1 + ")");

        LamportResult<String> resInfoRes;
        try {
            resInfoRes = reservationServer.getReservation(reservationId, sendL1);
            logicalClock.receiveEvent(resInfoRes.getTimestamp());
            log("RECEIVE", "ReservationServer getReservation response received (Reservation Lamport: " + resInfoRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "ReservationServer is unavailable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Could not verify reservation: ReservationServer is unavailable (" + e.getMessage() + ").", respL);
        }

        String reservationInfo = resInfoRes.getData();
        if (reservationInfo == null || reservationInfo.contains("not found")) {
            log("LOCAL", "Reservation validation failed.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Invalid Reservation ID: " + reservationId + " (not found or already cancelled).", respL);
        }

        long sendL2 = logicalClock.sendEvent();
        log("SEND", "Contacting ReservationServer.getReservationPort for " + reservationId + " (Lamport: " + sendL2 + ")");

        LamportResult<String> portRes;
        try {
            portRes = reservationServer.getReservationPort(reservationId, sendL2);
            logicalClock.receiveEvent(portRes.getTimestamp());
            log("RECEIVE", "ReservationServer getReservationPort response received: " + portRes.getData() + " (Reservation Lamport: " + portRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "Could not obtain charging port.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Could not determine charging port: ReservationServer is unavailable (" + e.getMessage() + ").", respL);
        }

        String portId = portRes.getData();
        if (portId == null || portId.equals("NONE")) {
            log("LOCAL", "No charging port associated with reservation.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("No charging port is associated with reservation " + reservationId + ".", respL);
        }

        long sendL3 = logicalClock.sendEvent();
        log("SEND", "Contacting ChargingStationServer.startPortCharging for " + portId + " (Lamport: " + sendL3 + ")");

        LamportResult<String> stationRes;
        try {
            stationRes = chargingStation.startPortCharging(portId, sendL3);
            logicalClock.receiveEvent(stationRes.getTimestamp());
            log("RECEIVE", "ChargingStationServer startPortCharging response: " + stationRes.getData() + " (Station Lamport: " + stationRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "ChargingStationServer is unavailable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Could not start charging: ChargingStationServer is unavailable (" + e.getMessage() + ").", respL);
        }

        String stationResult = stationRes.getData();
        if (stationResult.equals("PORT_NOT_FOUND")) {
            log("LOCAL", "Port does not exist: " + portId);
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Charging port " + portId + " does not exist on the station.", respL);
        }

        if (stationResult.equals("PORT_NOT_RESERVED")) {
            log("LOCAL", "Port is not reserved: " + portId);
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Charging port " + portId + " is not reserved and cannot start charging.", respL);
        }

        simulateProcessing(700);

        Instant startTime = Instant.now();
        String sessionId;
        String userId = extractUserId(reservationInfo);

        synchronized (this) {
            sessionId = "SESSION-" + sessionCounter++;
            reservationSessions.put(reservationId, sessionId);
            sessionStatus.put(sessionId, "CHARGING");
            sessionPort.put(sessionId, portId);
            sessionStartTimes.put(sessionId, startTime);
            sessionChargingPowers.put(sessionId, DEFAULT_CHARGING_POWER_KW);
            energyConsumed.put(sessionId, 0.0);
            if (userId != null) {
                sessionUserIds.put(sessionId, userId);
                lastBillingCheckpoints.put(sessionId, startTime);
            }
            logicalClock.tick();
            log("LOCAL", "Charging session " + sessionId + " started for Reservation " + reservationId
                    + " on Port " + portId + " at " + formatInstant(startTime)
                    + " (Charging Power: " + DEFAULT_CHARGING_POWER_KW + " kW)"
                    + (userId != null ? ", billed to Wallet[" + userId + "]" : ", WARNING: no userId found, wallet billing disabled for this session"));
        }

        persistStart(sessionId, reservationId, portId, startTime.toEpochMilli());
        replicateStart(sessionId, reservationId, portId, startTime.toEpochMilli());

        String result = "Charging Started Successfully!\n"
                + "Reservation ID: " + reservationId + "\n"
                + "Port: " + portId + "\n"
                + "Session ID: " + sessionId + "\n"
                + "Start Time: " + formatInstant(startTime) + "\n"
                + "Charging Power: " + DEFAULT_CHARGING_POWER_KW + " kW\n"
                + "Status: CHARGING";

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning START CHARGING response to client (Lamport: " + respL + ")");

        return new LamportResult<>(result, respL);
    }

    // =========================================================
    // STOP CHARGING (PRIMARY-only write)
    // =========================================================

    @Override
    public String stopCharging(String sessionId) throws RemoteException {
        return stopCharging(sessionId, 0).getData();
    }

    @Override
    public LamportResult<String> stopCharging(
            String sessionId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "STOP CHARGING request received for Session: " + sessionId + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        if (rejectIfSecondary("stopCharging")) {
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Stop charging failed: this instance is SECONDARY. Route to PRIMARY.", respL);
        }

        simulateProcessing(400);

        String result = stopChargingInternal(sessionId, "CLIENT_REQUESTED");

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning STOP CHARGING response to client (Lamport: " + respL + ")");

        return new LamportResult<>(result, respL);
    }

    // Shared stop logic used by both the client-invoked stopCharging RMI
    // method above and the automatic stop triggered by the billing cycle
    // below when a wallet's balance runs out. Lamport receive/response
    // wrapping stays with each caller since only stopCharging is itself
    // an RMI entry point -- this helper is a purely local operation.
    private String stopChargingInternal(String sessionId, String reason) {
        String currentStatus;
        Instant startTime;
        double power;

        synchronized (this) {
            if (!sessionStatus.containsKey(sessionId)) {
                log("LOCAL", "Session not found.");
                return "Session not found.";
            }
            currentStatus = sessionStatus.get(sessionId);
            startTime = sessionStartTimes.get(sessionId);
            power = sessionChargingPowers.getOrDefault(sessionId, DEFAULT_CHARGING_POWER_KW);
        }

        if (currentStatus.equals("COMPLETED")) {
            log("LOCAL", "Session is already completed.");
            return "Charging session is already completed.";
        }

        Instant endTime = Instant.now();
        long durationMillis = (startTime != null) ? Duration.between(startTime, endTime).toMillis() : 0;
        if (durationMillis < 0) {
            durationMillis = 0;
        }

        double durationSeconds = durationMillis / 1000.0;
        double durationHours = durationSeconds / 3600.0;
        double calculatedEnergy = power * durationHours;

        synchronized (this) {
            sessionEndTimes.put(sessionId, endTime);
            energyConsumed.put(sessionId, calculatedEnergy);
            sessionStatus.put(sessionId, "COMPLETED");
            logicalClock.tick();
            log("LOCAL", "Session " + sessionId + " STOPPED. Reason: " + reason
                    + ". ENERGY CALCULATION: Start=" + formatInstant(startTime)
                    + ", End=" + formatInstant(endTime)
                    + ", Duration=" + String.format("%.3f", durationSeconds) + "s"
                    + ", Power=" + power + " kW"
                    + ", Energy=" + String.format("%.4f", calculatedEnergy) + " kWh");
        }

        persistStop(sessionId, endTime.toEpochMilli(), calculatedEnergy);
        replicateStop(sessionId, endTime.toEpochMilli(), calculatedEnergy);

        return "Charging Stopped Successfully!\n"
                + "Session ID: " + sessionId + "\n"
                + "Port: " + sessionPort.getOrDefault(sessionId, "UNKNOWN") + "\n"
                + "Start Time: " + formatInstant(startTime) + "\n"
                + "End Time: " + formatInstant(endTime) + "\n"
                + "Charging Duration: " + String.format("%.3f", durationSeconds) + " seconds (" + String.format("%.4f", durationHours) + " hours)\n"
                + "Charging Power: " + power + " kW\n"
                + "Energy Consumed: " + String.format("%.4f", calculatedEnergy) + " kWh\n"
                + "Status: COMPLETED (" + reason + ")\n"
                + "Note: Port will be released after successful payment.";
    }

    // =========================================================
    // GET SESSION STATUS (read-only)
    // =========================================================

    @Override
    public String getSessionStatus(String sessionId) throws RemoteException {
        return getSessionStatus(sessionId, 0).getData();
    }

    @Override
    public LamportResult<String> getSessionStatus(
            String sessionId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET SESSION STATUS request for " + sessionId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            if (!sessionStatus.containsKey(sessionId)) {
                log("LOCAL", "Session not found.");
                long respL = logicalClock.sendEvent();
                return new LamportResult<>("Session not found.", respL);
            }

            String status = sessionStatus.get(sessionId);
            Instant start = sessionStartTimes.get(sessionId);
            Instant end = sessionEndTimes.get(sessionId);
            double power = sessionChargingPowers.getOrDefault(sessionId, DEFAULT_CHARGING_POWER_KW);
            String portId = sessionPort.getOrDefault(sessionId, "UNKNOWN");

            double energy;
            double durationSec;

            if (status.equals("COMPLETED")) {
                energy = energyConsumed.getOrDefault(sessionId, 0.0);
                long durMs = (start != null && end != null) ? Duration.between(start, end).toMillis() : 0;
                durationSec = durMs / 1000.0;
            } else {
                Instant now = Instant.now();
                long durMs = (start != null) ? Duration.between(start, now).toMillis() : 0;
                durationSec = durMs / 1000.0;
                energy = power * (durationSec / 3600.0);
            }

            logicalClock.tick();
            log("LOCAL", "Session ID " + sessionId + " status: " + status + " | Duration: " + String.format("%.3f", durationSec) + "s | Energy: " + String.format("%.4f", energy) + " kWh");

            String result = "Session ID: " + sessionId + "\n"
                    + "Port: " + portId + "\n"
                    + "Status: " + status + "\n"
                    + "Start Time: " + formatInstant(start) + "\n"
                    + "End Time: " + (end != null ? formatInstant(end) : "NOT COMPLETED (CHARGING)") + "\n"
                    + "Duration: " + String.format("%.3f", durationSec) + " seconds\n"
                    + "Charging Power: " + power + " kW\n"
                    + "Energy Consumed: " + String.format("%.4f", energy) + " kWh";

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET SESSION STATUS response (Lamport: " + respL + ")");
            return new LamportResult<>(result, respL);
        }
    }

    // =========================================================
    // GET SESSION PORT & ENERGY CONSUMED (read-only)
    // =========================================================

    @Override
    public String getSessionPort(String sessionId) throws RemoteException {
        return getSessionPort(sessionId, 0).getData();
    }

    @Override
    public LamportResult<String> getSessionPort(
            String sessionId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET SESSION PORT request for " + sessionId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(300);

        synchronized (this) {
            String portId = sessionPort.get(sessionId);
            if (portId == null) {
                portId = "NONE";
            }
            logicalClock.tick();
            log("LOCAL", "Session " + sessionId + " assigned port: " + portId);

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET SESSION PORT response " + portId + " (Lamport: " + respL + ")");
            return new LamportResult<>(portId, respL);
        }
    }

    @Override
    public double getEnergyConsumed(String sessionId) throws RemoteException {
        return getEnergyConsumed(sessionId, 0).getData();
    }

    @Override
    public LamportResult<Double> getEnergyConsumed(
            String sessionId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET ENERGY request for " + sessionId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            if (!energyConsumed.containsKey(sessionId)) {
                log("LOCAL", "Session not found.");
                long respL = logicalClock.sendEvent();
                return new LamportResult<>(-1.0, respL);
            }

            double energy = energyConsumed.get(sessionId);
            logicalClock.tick();
            log("LOCAL", "Returning energy consumed: " + String.format("%.4f", energy) + " kWh");

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET ENERGY response (Lamport: " + respL + ")");
            return new LamportResult<>(energy, respL);
        }
    }

    // =========================================================
    // WALLET BILLING (periodic, self-initiated -- the mechanism that
    // demonstrates cross-server Lamport clocks: this ScheduledExecutorService
    // ticks independently of any client request and, every cycle, sends a
    // fresh Lamport-stamped RMI call from this ChargingSession container
    // into the Payment container's Bully-elected leader, then folds the
    // Payment container's response Lamport value back into this clock.)
    // =========================================================

    public void startBillingScheduler() {
        billingScheduler = Executors.newSingleThreadScheduledExecutor();
        billingScheduler.scheduleAtFixedRate(this::runBillingCycle,
                BILLING_INTERVAL_SECONDS, BILLING_INTERVAL_SECONDS, TimeUnit.SECONDS);
        log("LOCAL", "Wallet billing scheduler started. Interval: " + BILLING_INTERVAL_SECONDS + "s.");
    }

    private void runBillingCycle() {
        if (!"PRIMARY".equals(role)) {
            return;
        }

        Set<String> chargingSessionIds;
        synchronized (this) {
            chargingSessionIds = new HashSet<>();
            for (Map.Entry<String, String> e : sessionStatus.entrySet()) {
                if ("CHARGING".equals(e.getValue()) && sessionUserIds.containsKey(e.getKey())) {
                    chargingSessionIds.add(e.getKey());
                }
            }
        }

        for (String sessionId : chargingSessionIds) {
            billSession(sessionId);
        }
    }

    private void billSession(String sessionId) {
        String userId;
        double power;
        Instant checkpoint;
        Instant now = Instant.now();

        synchronized (this) {
            if (!"CHARGING".equals(sessionStatus.get(sessionId))) return;
            userId = sessionUserIds.get(sessionId);
            power = sessionChargingPowers.getOrDefault(sessionId, DEFAULT_CHARGING_POWER_KW);
            checkpoint = lastBillingCheckpoints.getOrDefault(sessionId, sessionStartTimes.get(sessionId));
        }
        if (userId == null || checkpoint == null) return;

        long intervalMillis = Duration.between(checkpoint, now).toMillis();
        if (intervalMillis <= 0) return;
        double energySinceCheckpoint = power * (intervalMillis / 1000.0 / 3600.0);

        log("LOCAL", "BILLING_CYCLE: Session " + sessionId + " (User " + userId + ") consumed "
                + String.format("%.5f", energySinceCheckpoint) + " kWh since last checkpoint.");

        long sendL1 = logicalClock.sendEvent();
        log("SEND", "Contacting PricingServer.calculatePrice for incremental billing, Session " + sessionId + ", Energy " + String.format("%.5f", energySinceCheckpoint) + " kWh (Lamport: " + sendL1 + ")");

        double cost;
        try {
            LamportResult<Double> priceRes = pricing.calculatePrice(STATION_ID, energySinceCheckpoint, sendL1);
            logicalClock.receiveEvent(priceRes.getTimestamp());
            log("RECEIVE", "PricingServer calculatePrice response: Rs. " + priceRes.getData() + " (Pricing Lamport: " + priceRes.getTimestamp() + ")");
            cost = priceRes.getData();
            if (cost < 0) cost = 0;
        } catch (RemoteException e) {
            log("LOCAL", "WARNING: PricingServer unavailable during billing cycle: " + e.getMessage() + ". Skipping this cycle.");
            return;
        }

        long sendL2 = logicalClock.sendEvent();
        log("SEND", "Contacting PaymentServer.checkAndDeductBalance for User " + userId + ", Session " + sessionId + ", Amount Rs. " + String.format("%.2f", cost) + " (Lamport: " + sendL2 + ")");

        String walletResult;
        try {
            LamportResult<String> deductRes = payment.checkAndDeductBalance(userId, cost, sessionId, sendL2);
            logicalClock.receiveEvent(deductRes.getTimestamp());
            log("RECEIVE", "PaymentServer checkAndDeductBalance response: " + deductRes.getData() + " (Payment Lamport: " + deductRes.getTimestamp() + ")");
            walletResult = deductRes.getData();
        } catch (RemoteException e) {
            log("LOCAL", "WARNING: PaymentServer unavailable during billing cycle: " + e.getMessage() + ". Skipping this cycle (session continues).");
            return;
        }

        if (walletResult != null && walletResult.startsWith("OK")) {
            synchronized (this) {
                lastBillingCheckpoints.put(sessionId, now);
            }
            logicalClock.tick();
            log("BILLING_OK", "Session " + sessionId + " billed Rs. " + String.format("%.2f", cost) + " to User " + userId + ". " + walletResult);
        } else {
            logicalClock.tick();
            log("INSUFFICIENT_BALANCE", "Session " + sessionId + " User " + userId + " has insufficient balance (" + walletResult + "). Auto-stopping charging session.");
            stopChargingInternal(sessionId, "INSUFFICIENT_BALANCE");
        }
    }

    private void persistStart(String sessionId, String reservationId, String portId, long startTimeMs) {
        if (dao != null) {
            try {
                dao.insertSession(sessionId, reservationId, portId, DEFAULT_CHARGING_POWER_KW, startTimeMs);
                log("LOCAL", "[DB] Session " + sessionId + " persisted to database.");
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to persist session " + sessionId + ": " + dbEx.getMessage());
            }
        }
    }

    private void persistStop(String sessionId, long endTimeMs, double energyKwh) {
        if (dao != null) {
            try {
                dao.completeSession(sessionId, endTimeMs, energyKwh);
                log("LOCAL", "[DB] Session " + sessionId + " marked COMPLETED in database.");
            } catch (Exception dbEx) {
                log("LOCAL", "[DB] WARNING: Failed to update session " + sessionId + " in DB: " + dbEx.getMessage());
            }
        }
    }

    private void replicateStart(String sessionId, String reservationId, String portId, long startTimeMs) {
        if (managerClient != null) {
            managerClient.replicate(identity.serviceName, identity.serverId,
                    new StateDelta("SESSION_START", sessionId, reservationId, portId, DEFAULT_CHARGING_POWER_KW, startTimeMs),
                    logicalClock);
        }
    }

    private void replicateStop(String sessionId, long endTimeMs, double energyKwh) {
        if (managerClient != null) {
            managerClient.replicate(identity.serviceName, identity.serverId,
                    new StateDelta("SESSION_STOP", sessionId, endTimeMs, energyKwh),
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
        if ("SESSION_START".equals(delta.opType) && delta.args.length == 5) {
            String sessionId = (String) delta.args[0];
            String reservationId = (String) delta.args[1];
            String portId = (String) delta.args[2];
            double power = (Double) delta.args[3];
            long startMs = (Long) delta.args[4];
            reservationSessions.put(reservationId, sessionId);
            sessionStatus.put(sessionId, "CHARGING");
            sessionPort.put(sessionId, portId);
            sessionStartTimes.put(sessionId, Instant.ofEpochMilli(startMs));
            sessionChargingPowers.put(sessionId, power);
            energyConsumed.put(sessionId, 0.0);
            persistStart(sessionId, reservationId, portId, startMs);
            log("LOCAL", "REPLICATION_APPLIED: session start " + sessionId);
        } else if ("SESSION_STOP".equals(delta.opType) && delta.args.length == 3) {
            String sessionId = (String) delta.args[0];
            long endMs = (Long) delta.args[1];
            double energy = (Double) delta.args[2];
            sessionEndTimes.put(sessionId, Instant.ofEpochMilli(endMs));
            energyConsumed.put(sessionId, energy);
            sessionStatus.put(sessionId, "COMPLETED");
            persistStop(sessionId, endMs, energy);
            log("LOCAL", "REPLICATION_APPLIED: session stop " + sessionId);
        }
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(true, respL);
    }

    @Override
    public synchronized LamportResult<GenericSnapshot> getClusterSnapshot(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        GenericSnapshot snap = new GenericSnapshot();
        snap.put("sessionStatus", new HashMap<>(sessionStatus));
        snap.put("sessionPort", new HashMap<>(sessionPort));
        snap.put("energyConsumed", new HashMap<>(energyConsumed));
        long respL = logicalClock.sendEvent();
        return new LamportResult<>(snap, respL);
    }

    @Override
    @SuppressWarnings("unchecked")
    public synchronized LamportResult<Boolean> applyClusterSnapshot(GenericSnapshot snapshot, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        Map<String, String> restoredStatus = snapshot.get("sessionStatus");
        Map<String, String> restoredPort = snapshot.get("sessionPort");
        Map<String, Double> restoredEnergy = snapshot.get("energyConsumed");
        if (restoredStatus != null) sessionStatus.putAll(restoredStatus);
        if (restoredPort != null) sessionPort.putAll(restoredPort);
        if (restoredEnergy != null) energyConsumed.putAll(restoredEnergy);
        log("LOCAL", "FULL_SYNC_APPLIED: restored " + (restoredStatus == null ? 0 : restoredStatus.size()) + " sessions.");
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

            String reservationUrl = System.getenv("RESERVATION_URL");
            if (reservationUrl == null || reservationUrl.trim().isEmpty()) {
                String resHost = System.getenv("MANAGER_HOST");
                if (resHost == null || resHost.trim().isEmpty()) {
                    resHost = System.getenv("RESERVATION_HOST");
                    if (resHost == null || resHost.trim().isEmpty()) {
                        resHost = "localhost";
                    }
                }
                String resPort = System.getenv("MANAGER_PORT");
                if (resPort == null || resPort.trim().isEmpty()) {
                    resPort = System.getenv("RESERVATION_PORT");
                    if (resPort == null || resPort.trim().isEmpty()) {
                        resPort = "1240";
                    }
                }
                reservationUrl = "rmi://" + resHost + ":" + resPort + "/ReservationService";
            }

            ReservationInterface reservationServer = null;
            retryCount = 0;

            System.out.println("Connecting to ReservationServer at " + reservationUrl + "...");

            while (retryCount < maxRetries) {
                try {
                    reservationServer = (ReservationInterface) Naming.lookup(reservationUrl);
                    System.out.println("ReservationServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for ReservationServer... Retry " + retryCount + "/" + maxRetries + "...");
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }

            if (reservationServer == null) {
                System.out.println("Could not connect to ReservationServer.");
                return;
            }

            String paymentUrl = Common.ManagerRouting.resolvePaymentUrl();

            PaymentInterface payment = null;
            retryCount = 0;

            System.out.println("Connecting to PaymentServer at " + paymentUrl + "...");

            while (retryCount < maxRetries) {
                try {
                    payment = (PaymentInterface) Naming.lookup(paymentUrl);
                    System.out.println("PaymentServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for PaymentServer... Retry " + retryCount + "/" + maxRetries + "...");
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }

            if (payment == null) {
                System.out.println("Could not connect to PaymentServer.");
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

            ServerIdentity identity = ServerIdentity.fromEnvironment("ChargingSessionService", 1236, 2236, "PRIMARY");

            ChargingSessionServer server = new ChargingSessionServer(identity, reservationServer, chargingStation, payment, pricing);
            server.initWithDatabase();
            server.setManagerClient(new ClusterManagerClient());

            LocateRegistry.createRegistry(identity.registryPort);

            server.initCluster(selfHost);
            server.startBillingScheduler();

            String bindUrl = "rmi://localhost:" + identity.registryPort + "/" + identity.serviceName + "-" + identity.serverId;
            Naming.rebind(bindUrl, server);

            System.out.println("Charging Session Server instance " + identity.serverId + " bound to registry successfully.");
            System.out.println("Role: " + server.role + " | Bound: " + bindUrl + " | Peers: " + identity.peers);

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Waiting for client requests...");

        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}
