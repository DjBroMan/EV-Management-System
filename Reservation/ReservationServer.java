import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.CristianClient;
import Clock.DistributedLogger;
import Clock.LamportResult;

// RMI Server class that implements ReservationInterface
public class ReservationServer extends UnicastRemoteObject
        implements ReservationInterface {

    private static final long serialVersionUID = 1L;
    private static final String SERVER_NAME = "ReservationServer";

    private final LogicalClock logicalClock = new LogicalClock();

    // Stores reservation ID -> reservation details string
    private Map<String, String> reservations;

    // Stores reservation ID -> charging port ID
    private Map<String, String> reservationPorts;

    // Sequential counter for reservation IDs
    private int reservationCounter = 1001;

    // Remote reference to ChargingStationServer
    private ChargingStationInterface chargingStation;

    // Constructor
    protected ReservationServer(
            ChargingStationInterface chargingStation)
            throws RemoteException {

        super(2235);

        reservations = new HashMap<>();
        reservationPorts = new HashMap<>();

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
    // RESERVE SLOT
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
        log("RECEIVE", "RESERVE SLOT request received from User " + userId + ", Vehicle " + vehicleId + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

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

        long sendL = logicalClock.sendEvent();
        log("SEND", "Contacting ChargingStationServer.reserveAnyAvailablePort (Lamport: " + sendL + ")");

        LamportResult<String> stationRes;
        try {
            stationRes = chargingStation.reserveAnyAvailablePort(sendL);
            logicalClock.receiveEvent(stationRes.getTimestamp());
            log("RECEIVE", "ChargingStationServer returned port " + stationRes.getData() + " (Station Lamport: " + stationRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "ChargingStationServer is unavailable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Reservation failed: ChargingStationServer is unavailable (" + e.getMessage() + ").", respL);
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
        synchronized (this) {
            reservationId = "RES" + reservationCounter++;
        }

        String reservationDetails =
                "Reservation ID: " + reservationId +
                ", User ID: " + userId +
                ", Vehicle ID: " + vehicleId +
                ", Port: " + portId +
                ", Status: CONFIRMED";

        simulateProcessing(500);

        synchronized (this) {
            reservations.put(reservationId, reservationDetails);
            reservationPorts.put(reservationId, portId);
            logicalClock.tick();
            log("LOCAL", "Reservation RES" + reservationId + " stored successfully.");
        }

        String result = "Reservation successful!\n" + reservationDetails;
        long respL = logicalClock.sendEvent();
        log("SEND", "Returning RESERVE SLOT response to client (Lamport: " + respL + ")");

        return new LamportResult<>(result, respL);
    }

    // =========================================================
    // CANCEL RESERVATION
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
        log("RECEIVE", "CANCEL RESERVATION request for ID " + reservationId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        String portId = null;
        boolean found = false;

        synchronized (this) {
            if (reservations.containsKey(reservationId)) {
                found = true;
                portId = reservationPorts.remove(reservationId);
                reservations.remove(reservationId);
            }
        }

        if (found) {
            log("LOCAL", "Reservation found and removed from database. Assigned port: " + portId);

            if (portId != null) {
                long sendL = logicalClock.sendEvent();
                log("SEND", "Contacting ChargingStationServer.releasePort for port " + portId + " (Lamport: " + sendL + ")");

                try {
                    LamportResult<String> releaseRes = chargingStation.releasePort(portId, sendL);
                    logicalClock.receiveEvent(releaseRes.getTimestamp());
                    log("RECEIVE", "ChargingStationServer release response: " + releaseRes.getData() + " (Station Lamport: " + releaseRes.getTimestamp() + ")");
                } catch (RemoteException e) {
                    log("LOCAL", "WARNING: Could not release port " + portId + " on ChargingStationServer");
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
    // GET RESERVATION
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
        log("RECEIVE", "GET RESERVATION request for ID " + reservationId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            if (reservations.containsKey(reservationId)) {
                String details = reservations.get(reservationId);
                logicalClock.tick();
                log("LOCAL", "Reservation details found: " + details);

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
    // GET RESERVATION PORT
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
        log("RECEIVE", "GET RESERVATION PORT request for ID " + reservationId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            String portId = reservationPorts.get(reservationId);
            if (portId == null) {
                portId = "NONE";
            }

            logicalClock.tick();
            log("LOCAL", "Reservation ID " + reservationId + " is assigned to port: " + portId);

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET RESERVATION PORT response " + portId + " (Lamport: " + respL + ")");
            return new LamportResult<>(portId, respL);
        }
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

            LocateRegistry.createRegistry(1235);
            ReservationServer server = new ReservationServer(chargingStation);

            Naming.rebind("rmi://localhost:1235/ReservationService", server);

            System.out.println("=================================");
            System.out.println("   RESERVATION RMI SERVER");
            System.out.println("=================================");

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Waiting for reservation requests...");
            System.out.println("=================================");

        } catch (Exception e) {
            System.out.println("Server Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}