import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.util.HashMap;

// RMI Server class that implements the ChargingSessionInterface
public class ChargingSessionServer
        extends UnicastRemoteObject
        implements ChargingSessionInterface {

    // Stores reservation ID -> session ID
    private HashMap<String, String> reservationSessions;

    // Stores session ID -> session status
    private HashMap<String, String> sessionStatus;

    // Stores session ID -> energy consumed in kWh
    private HashMap<String, Double> energyConsumed;

    // Stores session ID -> charging port ID
    private HashMap<String, String> sessionPort;

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

        super();

        reservationSessions =
                new HashMap<String, String>();

        sessionStatus =
                new HashMap<String, String>();

        energyConsumed =
                new HashMap<String, Double>();

        sessionPort =
                new HashMap<String, String>();

        this.reservationServer =
                reservationServer;

        this.chargingStation =
                chargingStation;
    }

    // =========================================================
    // THREAD LOGGING METHOD
    // =========================================================

    private void log(String message) {

        System.out.println(
                "[Thread-" +
                Thread.currentThread().getId() +
                " | " +
                Thread.currentThread().getName() +
                "] " +
                message
        );
    }

    // =========================================================
    // SIMULATED PROCESSING DELAY
    // =========================================================

    private void simulateProcessing(long milliseconds) {

        try {

            Thread.sleep(milliseconds);

        } catch (InterruptedException e) {

            Thread.currentThread().interrupt();

            log("Thread interrupted during processing.");
        }
    }

    // =========================================================
    // START CHARGING
    // =========================================================

    public String startCharging(
            String reservationId)
            throws RemoteException {

        log("START CHARGING request received for Reservation: "
                + reservationId);

        // -----------------------------------------------------
        // Validate reservation ID
        // -----------------------------------------------------

        log("Validating Reservation ID...");

        simulateProcessing(400);

        if (reservationId == null ||
                reservationId.length() == 0) {

            log("Task failed: Invalid Reservation ID.");

            return "Invalid Reservation ID.";
        }

        // -----------------------------------------------------
        // Check whether session already exists
        // -----------------------------------------------------

        log("Checking whether a charging session already exists.");

        simulateProcessing(400);

        synchronized (this) {
            if (reservationSessions.containsKey(
                    reservationId)) {

                String existingSession =
                        reservationSessions.get(
                                reservationId);

                log("Existing session found: "
                        + existingSession);

                return "Charging session already exists.\n"
                        + "Session ID: "
                        + existingSession;
            }
        }

        // -----------------------------------------------------
        // STEP 1: Verify reservation
        // -----------------------------------------------------

        log("Contacting ReservationServer "
                + "to verify reservation.");

        simulateProcessing(500);

        String reservationInfo;

        try {

            reservationInfo =
                    reservationServer.getReservation(
                            reservationId
                    );

            log("ReservationServer response received.");

        } catch (RemoteException e) {

            log("ReservationServer is unavailable.");

            return "Could not verify reservation: "
                    + "ReservationServer is unavailable ("
                    + e.getMessage()
                    + ").";
        }

        if (reservationInfo == null ||
                reservationInfo.contains("not found")) {

            log("Reservation validation failed.");

            return "Invalid Reservation ID: "
                    + reservationId
                    + " (not found or already cancelled).";
        }

        log("Reservation verified successfully.");

        // -----------------------------------------------------
        // STEP 2: Determine charging port
        // -----------------------------------------------------

        log("Requesting charging port "
                + "from ReservationServer.");

        simulateProcessing(500);

        String portId;

        try {

            portId =
                    reservationServer.getReservationPort(
                            reservationId
                    );

            log("Charging port received: "
                    + portId);

        } catch (RemoteException e) {

            log("Could not obtain charging port.");

            return "Could not determine charging port: "
                    + "ReservationServer is unavailable ("
                    + e.getMessage()
                    + ").";
        }

        if (portId == null ||
                portId.equals("NONE")) {

            log("No charging port associated "
                    + "with reservation.");

            return "No charging port is associated "
                    + "with reservation "
                    + reservationId
                    + ".";
        }

        // -----------------------------------------------------
        // STEP 3: Start charging at station
        // -----------------------------------------------------

        log("Contacting ChargingStationServer.");

        simulateProcessing(500);

        String stationResult;

        try {

            stationResult =
                    chargingStation.startPortCharging(
                            portId
                    );

            log("ChargingStationServer response: "
                    + stationResult);

        } catch (RemoteException e) {

            log("ChargingStationServer is unavailable.");

            return "Could not start charging: "
                    + "ChargingStationServer is unavailable ("
                    + e.getMessage()
                    + ").";
        }

        if (stationResult.equals(
                "PORT_NOT_FOUND")) {

            log("Port does not exist: "
                    + portId);

            return "Charging port "
                    + portId
                    + " does not exist on the station.";
        }

        if (stationResult.equals(
                "PORT_NOT_RESERVED")) {

            log("Port is not reserved: "
                    + portId);

            return "Charging port "
                    + portId
                    + " is not reserved "
                    + "and cannot start charging.";
        }

        // -----------------------------------------------------
        // STEP 4: Create charging session
        // -----------------------------------------------------

        log("Charging station confirmed "
                + "that charging can start.");

        log("Creating charging session...");

        simulateProcessing(700);

        String sessionId;

        synchronized (this) {
            sessionId =
                    "SESSION-" + sessionCounter++;
        }

        log("Generated Session ID: "
                + sessionId);

        // -----------------------------------------------------
        // Store session information
        // -----------------------------------------------------

        log("Storing session information...");

        simulateProcessing(500);

        synchronized (this) {
            reservationSessions.put(
                    reservationId,
                    sessionId
            );

            sessionStatus.put(
                    sessionId,
                    "CHARGING"
            );

            energyConsumed.put(
                    sessionId,
                    0.0
            );

            sessionPort.put(
                    sessionId,
                    portId
            );
        }

        log("Charging session created successfully.");

        log("Reservation: "
                + reservationId);

        log("Port: "
                + portId);

        log("Session: "
                + sessionId);

        log("Status: CHARGING");

        log("START CHARGING task completed.");

        return "Charging Started Successfully!\n"
                + "Reservation ID: "
                + reservationId
                + "\n"
                + "Port: "
                + portId
                + "\n"
                + "Session ID: "
                + sessionId
                + "\n"
                + "Status: CHARGING";
    }

    // =========================================================
    // STOP CHARGING
    // =========================================================

    public String stopCharging(
            String sessionId)
            throws RemoteException {

        log("STOP CHARGING request received for Session: "
                + sessionId);

        // -----------------------------------------------------
        // Check session
        // -----------------------------------------------------

        log("Checking charging session...");

        simulateProcessing(400);

        String currentStatus;

        synchronized (this) {
            if (!sessionStatus.containsKey(
                    sessionId)) {

                log("Session not found.");

                return "Session not found.";
            }

            currentStatus =
                    sessionStatus.get(
                            sessionId);
        }

        log("Current session status: "
                + currentStatus);

        // -----------------------------------------------------
        // Check if already completed
        // -----------------------------------------------------

        if (currentStatus.equals(
                "COMPLETED")) {

            log("Session is already completed.");

            return "Charging session is already completed.";
        }

        // -----------------------------------------------------
        // Calculate energy
        // -----------------------------------------------------

        log("Calculating energy consumed...");

        simulateProcessing(700);

        // Demonstration value: 25.0 kWh
        double recordedEnergy = 25.0;

        // -----------------------------------------------------
        // Update session status (Port release happens after payment)
        // -----------------------------------------------------

        log("Updating session status to COMPLETED...");

        simulateProcessing(500);

        synchronized (this) {
            energyConsumed.put(
                    sessionId,
                    recordedEnergy
            );

            sessionStatus.put(
                    sessionId,
                    "COMPLETED"
            );
        }

        log("Session status changed to COMPLETED.");

        log("STOP CHARGING task completed. Port remains locked until payment.");

        return "Charging Stopped Successfully!\n"
                + "Session ID: "
                + sessionId
                + "\n"
                + "Energy Consumed: "
                + recordedEnergy
                + " kWh\n"
                + "Status: COMPLETED\n"
                + "Note: Port will be released after successful payment.";
    }

    // =========================================================
    // GET SESSION STATUS
    // =========================================================

    public String getSessionStatus(
            String sessionId)
            throws RemoteException {

        log("GET SESSION STATUS request received for: "
                + sessionId);

        log("Searching session database...");

        simulateProcessing(400);

        synchronized (this) {
            if (!sessionStatus.containsKey(
                    sessionId)) {

                log("Session not found.");

                return "Session not found.";
            }

            String status =
                    sessionStatus.get(
                            sessionId);

            double energy =
                    energyConsumed.get(
                            sessionId);

            log("Returning session status: "
                    + status);

            log("Energy consumed: "
                    + energy
                    + " kWh");

            log("GET SESSION STATUS task completed.");

            return "Session ID: "
                    + sessionId
                    + "\n"
                    + "Status: "
                    + status
                    + "\n"
                    + "Energy Consumed: "
                    + energy
                    + " kWh";
        }
    }

    // =========================================================
    // GET ENERGY CONSUMED
    // =========================================================

    public String getSessionPort(
            String sessionId)
            throws RemoteException {

        log("GET SESSION PORT request received for: "
                + sessionId);

        simulateProcessing(300);

        synchronized (this) {
            if (!sessionPort.containsKey(sessionId)) {
                log("Session port not found for: " + sessionId);
                return "NONE";
            }
            String portId = sessionPort.get(sessionId);
            log("Session " + sessionId + " is assigned to port: " + portId);
            return portId;
        }
    }

    public double getEnergyConsumed(
            String sessionId)
            throws RemoteException {

        log("GET ENERGY request received for: "
                + sessionId);

        log("Searching energy database...");

        simulateProcessing(400);

        synchronized (this) {
            if (!energyConsumed.containsKey(
                    sessionId)) {

                log("Session not found.");

                return -1;
            }

            double energy =
                    energyConsumed.get(
                            sessionId);

            log("Returning energy consumed: "
                    + energy
                    + " kWh");

            log("GET ENERGY task completed.");

            return energy;
        }
    }

    // =========================================================
    // MAIN
    // =========================================================

    public static void main(String[] args) {

        final String HOST =
                "rmi://localhost:1236/ChargingSessionServer";

        try {

            // -------------------------------------------------
            // Connect to ChargingStationServer
            // -------------------------------------------------

            ChargingStationInterface chargingStation;

            try {

                chargingStation =
                        (ChargingStationInterface)
                        Naming.lookup(
                                "rmi://localhost:1234//ChargingStationServer"
                        );

            } catch (Exception e) {

                System.out.println(
                        "Could not connect to "
                        + "ChargingStationServer."
                );

                System.out.println(
                        "Please start ChargingStationServer "
                        + "on port 1234."
                );

                return;
            }

            // -------------------------------------------------
            // Connect to ReservationServer
            // -------------------------------------------------

            ReservationInterface reservationServer;

            try {

                reservationServer =
                        (ReservationInterface)
                        Naming.lookup(
                                "rmi://localhost:1235/ReservationService"
                        );

            } catch (Exception e) {

                System.out.println(
                        "Could not connect to "
                        + "ReservationServer."
                );

                System.out.println(
                        "Please start ReservationServer "
                        + "on port 1235."
                );

                return;
            }

            // -------------------------------------------------
            // Start server
            // -------------------------------------------------

            System.out.println(
                    "Starting Charging Session Server.........."
            );

            ChargingSessionServer server =
                    new ChargingSessionServer(
                            reservationServer,
                            chargingStation
                    );

            System.out.println(
                    "Charging Session Server Instance Created......."
            );

            // Create RMI registry
            LocateRegistry.createRegistry(1236);

            // Bind server
            Naming.bind(
                    HOST,
                    server
            );

            System.out.println(
                    "Charging Session Server bound "
                    + "to registry successfully.."
            );

            System.out.println(
                    "Charging Session Server Ready...."
            );

            System.out.println(
                    "Simulated processing delays: ENABLED"
            );

            System.out.println(
                    "Waiting for client requests..."
            );

        } catch (Exception ex) {

            System.out.println(
                    "Exception : "
                    + ex.getMessage()
            );

            ex.printStackTrace();
        }
    }
}