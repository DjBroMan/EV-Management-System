import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.util.HashMap;
import java.time.Instant;
import java.time.Duration;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.CristianClient;
import Clock.DistributedLogger;
import Clock.LamportResult;

// RMI Server class that implements the ChargingSessionInterface with real-time session duration and energy calculations
public class ChargingSessionServer
        extends UnicastRemoteObject
        implements ChargingSessionInterface {

    private static final String SERVER_NAME = "ChargingSessionServer";
    private static final double DEFAULT_CHARGING_POWER_KW = 7.2;

    private static final DateTimeFormatter TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.systemDefault());

    private final LogicalClock logicalClock = new LogicalClock();

    // Stores reservation ID -> session ID
    private HashMap<String, String> reservationSessions;

    // Stores session ID -> session status
    private HashMap<String, String> sessionStatus;

    // Stores session ID -> energy consumed in kWh
    private HashMap<String, Double> energyConsumed;

    // Stores session ID -> charging port ID
    private HashMap<String, String> sessionPort;

    // Real-time physical time tracking for charging duration and energy calculation
    private HashMap<String, Instant> sessionStartTimes;
    private HashMap<String, Instant> sessionEndTimes;
    private HashMap<String, Double> sessionChargingPowers;

    // Used to generate unique session IDs
    private int sessionCounter = 1001;

    // Remote references obtained on startup
    private ReservationInterface reservationServer;
    private ChargingStationInterface chargingStation;

    // Constructor
    public ChargingSessionServer(
            ReservationInterface reservationServer,
            ChargingStationInterface chargingStation)
            throws RemoteException {

        super(2236);

        reservationSessions = new HashMap<String, String>();
        sessionStatus = new HashMap<String, String>();
        energyConsumed = new HashMap<String, Double>();
        sessionPort = new HashMap<String, String>();

        sessionStartTimes = new HashMap<String, Instant>();
        sessionEndTimes = new HashMap<String, Instant>();
        sessionChargingPowers = new HashMap<String, Double>();

        this.reservationServer = reservationServer;
        this.chargingStation = chargingStation;
    }

    private void log(String message) {
        DistributedLogger.log(SERVER_NAME, logicalClock, message);
    }

    private void log(String eventType, String message) {
        DistributedLogger.log(SERVER_NAME, logicalClock, eventType, message);
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
    // START CHARGING
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

        // STEP 1: Verify reservation
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

        // STEP 2: Determine charging port
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

        // STEP 3: Start charging at station
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

        // STEP 4: Create charging session and record start time
        simulateProcessing(700);

        Instant startTime = Instant.now();
        String sessionId;

        synchronized (this) {
            sessionId = "SESSION-" + sessionCounter++;
            reservationSessions.put(reservationId, sessionId);
            sessionStatus.put(sessionId, "CHARGING");
            sessionPort.put(sessionId, portId);
            sessionStartTimes.put(sessionId, startTime);
            sessionChargingPowers.put(sessionId, DEFAULT_CHARGING_POWER_KW);
            energyConsumed.put(sessionId, 0.0);
            logicalClock.tick();
            log("LOCAL", "Charging session " + sessionId + " started for Reservation " + reservationId + " on Port " + portId + " at " + formatInstant(startTime) + " (Charging Power: " + DEFAULT_CHARGING_POWER_KW + " kW)");
        }

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
    // STOP CHARGING
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

        simulateProcessing(400);

        String currentStatus;
        Instant startTime;
        double power;

        synchronized (this) {
            if (!sessionStatus.containsKey(sessionId)) {
                log("LOCAL", "Session not found.");
                long respL = logicalClock.sendEvent();
                return new LamportResult<>("Session not found.", respL);
            }
            currentStatus = sessionStatus.get(sessionId);
            startTime = sessionStartTimes.get(sessionId);
            power = sessionChargingPowers.getOrDefault(sessionId, DEFAULT_CHARGING_POWER_KW);
        }

        if (currentStatus.equals("COMPLETED")) {
            log("LOCAL", "Session is already completed.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Charging session is already completed.", respL);
        }

        simulateProcessing(700);

        // Record physical end time and calculate elapsed duration & energy consumed
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
            log("LOCAL", "Session " + sessionId + " ENERGY CALCULATION: Start=" + formatInstant(startTime) + ", End=" + formatInstant(endTime) + ", Duration=" + String.format("%.3f", durationSeconds) + "s, Power=" + power + " kW, Energy=" + String.format("%.4f", calculatedEnergy) + " kWh");
        }

        String result = "Charging Stopped Successfully!\n"
                + "Session ID: " + sessionId + "\n"
                + "Port: " + sessionPort.getOrDefault(sessionId, "UNKNOWN") + "\n"
                + "Start Time: " + formatInstant(startTime) + "\n"
                + "End Time: " + formatInstant(endTime) + "\n"
                + "Charging Duration: " + String.format("%.3f", durationSeconds) + " seconds (" + String.format("%.4f", durationHours) + " hours)\n"
                + "Charging Power: " + power + " kW\n"
                + "Energy Consumed: " + String.format("%.4f", calculatedEnergy) + " kWh\n"
                + "Status: COMPLETED\n"
                + "Note: Port will be released after successful payment.";

        long respL = logicalClock.sendEvent();
        log("SEND", "Returning STOP CHARGING response to client (Lamport: " + respL + ")");

        return new LamportResult<>(result, respL);
    }

    // =========================================================
    // GET SESSION STATUS
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
    // GET SESSION PORT & ENERGY CONSUMED
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
    // MAIN
    // =========================================================

    public static void main(String[] args) {
        final String HOST = "rmi://localhost:1236/ChargingSessionServer";

        try {
            String rmiHost = System.getenv("RMI_SERVER_HOST");
            if (rmiHost != null && !rmiHost.trim().isEmpty()) {
                System.setProperty("java.rmi.server.hostname", rmiHost);
            }

            String stationUrl = System.getenv("STATION_URL");
            if (stationUrl == null || stationUrl.trim().isEmpty()) {
                String stationHost = System.getenv("STATION_HOST");
                if (stationHost == null || stationHost.trim().isEmpty()) {
                    stationHost = "localhost";
                }
                stationUrl = "rmi://" + stationHost + ":1234//ChargingStationServer";
            }

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

            ChargingSessionServer server = new ChargingSessionServer(reservationServer, chargingStation);

            LocateRegistry.createRegistry(1236);

            Naming.bind(HOST, server);

            System.out.println("Charging Session Server bound to registry successfully.");

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