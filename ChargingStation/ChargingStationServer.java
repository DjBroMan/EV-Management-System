import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.CristianClient;
import Clock.DistributedLogger;
import Clock.LamportResult;

public class ChargingStationServer
        extends UnicastRemoteObject
        implements ChargingStationInterface {

    private static final String SERVER_NAME = "ChargingStationServer";
    private final LogicalClock logicalClock = new LogicalClock();

    private String[] ports = { "P1", "P2", "P3", "P4" };
    private String[] portStatus = { "AVAILABLE", "AVAILABLE", "AVAILABLE", "AVAILABLE" };

    // Database access object — null when DB is not configured or unavailable
    private ChargingStationDAO dao = null;

    public ChargingStationServer() throws RemoteException {
        super(2234);
    }

    /**
     * Connects to the MySQL database, self-seeds P1–P4 if the table is empty,
     * and then loads current port statuses into the in-memory portStatus[] array.
     * Safe to call even when DB is not configured: logs a warning and returns.
     */
    public void initWithDatabase() {
        if (!DBConnectionHelper.isDatabaseConfigured()) {
            System.out.println("[DB:ChargingStationServer] DB_HOST not set. Running without database persistence.");
            return;
        }
        try {
            this.dao = new ChargingStationDAO();
            // Self-seed P1-P4 on first startup (Option A)
            dao.initPortsIfEmpty(ports, portStatus, "EV-STATION-01",
                    Clock.PhysicalClock.getSynchronizedPhysicalTimeMillis());
            // Load current port statuses from DB into portStatus[]
            java.util.Map<String, String> dbStatuses = dao.loadAllPorts();
            for (int i = 0; i < ports.length; i++) {
                String dbStatus = dbStatuses.get(ports[i]);
                if (dbStatus != null) {
                    portStatus[i] = dbStatus;
                }
            }
            System.out.println("[DB:ChargingStationServer] Database initialized successfully.");
        } catch (Exception e) {
            System.out.println("[DB:ChargingStationServer] WARNING: Database init failed: " + e.getMessage()
                    + ". Continuing without DB persistence.");
            this.dao = null;
        }
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

    private int indexOfPort(String portId) {
        for (int i = 0; i < ports.length; i++) {
            if (ports[i].equalsIgnoreCase(portId)) {
                return i;
            }
        }
        return -1;
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
    // STATION STATUS
    // =========================================================

    @Override
    public String getStationStatus() throws RemoteException {
        return getStationStatus(0).getData();
    }

    @Override
    public synchronized LamportResult<String> getStationStatus(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET STATION STATUS request received from client (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        int availablePorts = 0;
        for (String status : portStatus) {
            if (status.equals("AVAILABLE")) {
                availablePorts++;
            }
        }

        logicalClock.tick();
        log("LOCAL", "Available ports: " + availablePorts + "/" + ports.length);

        String result = "Station EV-STATION-01: " + availablePorts + " of " + ports.length + " ports available.";
        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning GET STATION STATUS response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // AVAILABLE PORTS
    // =========================================================

    @Override
    public String getAvailablePorts() throws RemoteException {
        return getAvailablePorts(0).getData();
    }

    @Override
    public synchronized LamportResult<String> getAvailablePorts(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET AVAILABLE PORTS request received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        String result = "Available Ports: ";
        boolean found = false;

        for (int i = 0; i < ports.length; i++) {
            if (portStatus[i].equals("AVAILABLE")) {
                result += ports[i] + " ";
                found = true;
            }
        }

        if (!found) {
            result = "No ports are currently available.";
        }

        logicalClock.tick();
        log("LOCAL", "Available ports: " + result);

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning GET AVAILABLE PORTS response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // CHECK PORT
    // =========================================================

    @Override
    public String checkPortAvailability(String portId) throws RemoteException {
        return checkPortAvailability(portId, 0).getData();
    }

    @Override
    public synchronized LamportResult<String> checkPortAvailability(String portId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "CHECK PORT request for " + portId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        int i = indexOfPort(portId);
        String result;
        if (i == -1) {
            result = "Port " + portId + " does not exist.";
        } else {
            result = "Port " + ports[i] + " is " + portStatus[i] + ".";
        }

        logicalClock.tick();
        log("LOCAL", "Port status check result: " + result);

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning CHECK PORT response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // RESERVE PORT
    // =========================================================

    @Override
    public String reservePort(String portId) throws RemoteException {
        return reservePort(portId, 0).getData();
    }

    @Override
    public synchronized LamportResult<String> reservePort(String portId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "RESERVE PORT request for " + portId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(700);

        int i = indexOfPort(portId);
        String result;

        if (i == -1) {
            result = "Port " + portId + " does not exist.";
        } else if (!portStatus[i].equals("AVAILABLE")) {
            result = "Port " + ports[i] + " is currently " + portStatus[i] + " and cannot be reserved.";
        } else {
            portStatus[i] = "RESERVED";
            result = "Port " + ports[i] + " reserved successfully.";
            logicalClock.tick();
            log("LOCAL", "Port " + ports[i] + " status changed to RESERVED.");
            final String reservedPortId = ports[i];
            if (dao != null) {
                try {
                    dao.updatePortStatus(reservedPortId, "RESERVED",
                            Clock.PhysicalClock.getSynchronizedPhysicalTimeMillis());
                } catch (Exception dbEx) {
                    log("LOCAL", "[DB] WARNING: Failed to persist RESERVED status for port " + reservedPortId
                            + ": " + dbEx.getMessage());
                }
            }
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning RESERVE PORT response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // RELEASE PORT
    // =========================================================

    @Override
    public String releasePort(String portId) throws RemoteException {
        return releasePort(portId, 0).getData();
    }

    @Override
    public synchronized LamportResult<String> releasePort(String portId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "RELEASE PORT request for " + portId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        int i = indexOfPort(portId);
        String result;

        if (i == -1) {
            result = "Port " + portId + " does not exist.";
        } else {
            portStatus[i] = "AVAILABLE";
            result = "Port " + ports[i] + " released successfully. Now AVAILABLE.";
            logicalClock.tick();
            log("LOCAL", "Port " + portId + " status changed to AVAILABLE.");
            final String releasedPortId = ports[i];
            if (dao != null) {
                try {
                    dao.updatePortStatus(releasedPortId, "AVAILABLE",
                            Clock.PhysicalClock.getSynchronizedPhysicalTimeMillis());
                } catch (Exception dbEx) {
                    log("LOCAL", "[DB] WARNING: Failed to persist AVAILABLE status for port " + releasedPortId
                            + ": " + dbEx.getMessage());
                }
            }
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning RELEASE PORT response (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
    }

    // =========================================================
    // RESERVE ANY AVAILABLE PORT
    // =========================================================

    @Override
    public String reserveAnyAvailablePort() throws RemoteException {
        return reserveAnyAvailablePort(0).getData();
    }

    @Override
    public synchronized LamportResult<String> reserveAnyAvailablePort(long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "RESERVE ANY AVAILABLE PORT request received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(700);

        for (int i = 0; i < ports.length; i++) {
            if (portStatus[i].equals("AVAILABLE")) {
                portStatus[i] = "RESERVED";
                logicalClock.tick();
                log("LOCAL", "Allocated available port " + ports[i] + " -> RESERVED");
                final String allocatedPortId = ports[i];
                if (dao != null) {
                    try {
                        dao.updatePortStatus(allocatedPortId, "RESERVED",
                                Clock.PhysicalClock.getSynchronizedPhysicalTimeMillis());
                    } catch (Exception dbEx) {
                        log("LOCAL", "[DB] WARNING: Failed to persist RESERVED status for port " + allocatedPortId
                                + ": " + dbEx.getMessage());
                    }
                }

                long sendL = logicalClock.sendEvent();
                log("SEND", "Returning allocated port " + allocatedPortId + " (Lamport: " + sendL + ")");
                return new LamportResult<>(allocatedPortId, sendL);
            }
        }

        logicalClock.tick();
        log("LOCAL", "No available charging ports found.");

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning NONE (Lamport: " + sendL + ")");

        return new LamportResult<>("NONE", sendL);
    }

    // =========================================================
    // START CHARGING
    // =========================================================

    @Override
    public String startPortCharging(String portId) throws RemoteException {
        return startPortCharging(portId, 0).getData();
    }

    @Override
    public synchronized LamportResult<String> startPortCharging(String portId, long clientLamport) throws RemoteException {
        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "START PORT CHARGING request for " + portId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(700);

        int i = indexOfPort(portId);
        String result;

        if (i == -1) {
            result = "PORT_NOT_FOUND";
        } else if (!portStatus[i].equals("RESERVED")) {
            result = "PORT_NOT_RESERVED";
        } else {
            portStatus[i] = "CHARGING";
            result = "CHARGING_STARTED";
            logicalClock.tick();
            log("LOCAL", "Port " + portId + " status changed to CHARGING.");
            if (dao != null) {
                try {
                    dao.updatePortStatus(portId, "CHARGING",
                            Clock.PhysicalClock.getSynchronizedPhysicalTimeMillis());
                } catch (Exception dbEx) {
                    log("LOCAL", "[DB] WARNING: Failed to persist CHARGING status for port " + portId
                            + ": " + dbEx.getMessage());
                }
            }
        }

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning START PORT CHARGING response " + result + " (Lamport: " + sendL + ")");

        return new LamportResult<>(result, sendL);
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

            final String HOST = "rmi://localhost:1234//ChargingStationServer";

            LocateRegistry.createRegistry(1234);

            ChargingStationServer server = new ChargingStationServer();

            // Initialize database: self-seed ports if empty, load statuses from DB
            server.initWithDatabase();

            Naming.bind(HOST, server);

            System.out.println("Charging Station Server is running...");
            System.out.println("Station: EV-STATION-01");
            System.out.println("RMI Registry running on port 1234.");

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Waiting for client requests...");
        } catch (Exception e) {
            System.out.println("Server Exception: " + e);
        }
    }
}