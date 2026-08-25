import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

// RMI Server class that implements ReservationInterface
public class ReservationServer extends UnicastRemoteObject
        implements ReservationInterface {

    private static final long serialVersionUID = 1L;

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

        super();

        reservations = new HashMap<>();
        reservationPorts = new HashMap<>();

        this.chargingStation = chargingStation;
    }

    // =========================================================
    // THREAD LOGGING
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
    // RESERVE SLOT
    // =========================================================

    // IMPORTANT:
    // This method is intentionally NOT synchronized.
    //
    // This allows multiple EV clients to enter the method
    // concurrently.
    //
    // The actual shared charging-port resource is protected
    // by ChargingStationServer.reserveAnyAvailablePort(),
    // which remains synchronized.
    //
    @Override
    public String reserveSlot(
            String userId,
            String vehicleId)
            throws RemoteException {

        log("========================================");
        log("RESERVE SLOT request received.");
        log("User ID: " + userId);
        log("Vehicle ID: " + vehicleId);

        // -----------------------------------------------------
        // Validate input
        // -----------------------------------------------------

        log("Validating user and vehicle information...");

        simulateProcessing(500);

        if (userId == null ||
                userId.trim().isEmpty()) {

            log("Reservation failed: Invalid User ID.");

            return "Reservation failed: Invalid User ID.";
        }

        if (vehicleId == null ||
                vehicleId.trim().isEmpty()) {

            log("Reservation failed: Invalid Vehicle ID.");

            return "Reservation failed: Invalid Vehicle ID.";
        }

        // -----------------------------------------------------
        // Ask ChargingStationServer for a port
        // -----------------------------------------------------

        log("Contacting ChargingStationServer.");

        log("Requesting any available charging port...");

        String portId;

        try {

            // IMPORTANT:
            // ChargingStationServer handles synchronization
            // for the shared charging ports.
            //
            // Multiple ReservationServer threads can reach
            // this call concurrently, but the station will
            // ensure that two threads cannot reserve the same
            // port.

            portId =
                    chargingStation.reserveAnyAvailablePort();

            log("ChargingStationServer returned: "
                    + portId);

        } catch (RemoteException e) {

            log("ChargingStationServer is unavailable.");

            return "Reservation failed: "
                    + "ChargingStationServer is unavailable ("
                    + e.getMessage()
                    + ").";
        }

        // -----------------------------------------------------
        // Check if a port was available
        // -----------------------------------------------------

        if (portId == null ||
                portId.equals("NONE")) {

            log("No charging ports are available.");

            return "Reservation failed: "
                    + "No charging ports available.";
        }

        log("Charging port successfully allocated: "
                + portId);

        // -----------------------------------------------------
        // Simulate reservation processing
        // -----------------------------------------------------

        log("Processing reservation request...");

        simulateProcessing(700);

        // -----------------------------------------------------
        // Generate reservation ID
        // -----------------------------------------------------

        String reservationId;

        /*
         * Only the shared counter needs synchronization.
         *
         * We do NOT synchronize the entire method because
         * doing that would make all EV requests wait for one
         * another.
         */
        synchronized (this) {

            reservationId =
                    "RES" + reservationCounter++;

            log("Generated Reservation ID: "
                    + reservationId);
        }

        // -----------------------------------------------------
        // Create reservation details
        // -----------------------------------------------------

        String reservationDetails =
                "Reservation ID: " + reservationId +
                ", User ID: " + userId +
                ", Vehicle ID: " + vehicleId +
                ", Port: " + portId +
                ", Status: CONFIRMED";

        log("Creating reservation record...");

        simulateProcessing(500);

        // -----------------------------------------------------
        // Store reservation
        // -----------------------------------------------------

        /*
         * HashMap is not thread-safe.
         *
         * Therefore the shared maps are protected with a
         * small synchronized block.
         *
         * The rest of the method remains concurrent.
         */
        synchronized (this) {

            reservations.put(
                    reservationId,
                    reservationDetails
            );

            reservationPorts.put(
                    reservationId,
                    portId
            );

            log("Reservation stored successfully.");

            log("Reservation ID: "
                    + reservationId);

            log("Assigned Port: "
                    + portId);
        }

        log("RESERVE SLOT task completed.");
        log("========================================");

        return "Reservation successful!\n"
                + reservationDetails;
    }

    // =========================================================
    // CANCEL RESERVATION
    // =========================================================

    @Override
    public String cancelReservation(
            String reservationId)
            throws RemoteException {

        log("CANCEL RESERVATION request received.");

        log("Reservation ID: "
                + reservationId);

        // -----------------------------------------------------
        // Check reservation
        // -----------------------------------------------------

        log("Checking reservation...");

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

            log("Reservation found and removed from database.");
            log("Assigned port: " + portId);

            // -------------------------------------------------
            // Release port
            // -------------------------------------------------

            if (portId != null) {

                log("Contacting ChargingStationServer "
                        + "to release port "
                        + portId);

                simulateProcessing(500);

                try {

                    String releaseResult =
                            chargingStation.releasePort(
                                    portId
                            );

                    log("ChargingStationServer response: "
                            + releaseResult);

                } catch (RemoteException e) {

                    log("WARNING: Could not release port "
                            + portId);

                    System.out.println(
                            "Warning: could not release port "
                            + portId
                            + " on ChargingStationServer: "
                            + e.getMessage()
                    );
                }
            }

            log("CANCEL RESERVATION task completed.");

            return "Reservation "
                    + reservationId
                    + " cancelled successfully.";
        }

        log("Reservation not found.");

        return "Reservation "
                + reservationId
                + " not found.";
    }

    // =========================================================
    // GET RESERVATION
    // =========================================================

    @Override
    public String getReservation(
            String reservationId)
            throws RemoteException {

        log("GET RESERVATION request received.");

        log("Reservation ID: "
                + reservationId);

        log("Searching reservation database...");

        simulateProcessing(400);

        synchronized (this) {
            if (reservations.containsKey(reservationId)) {

                log("Reservation found.");

                String details =
                        reservations.get(
                                reservationId
                        );

                log("Returning reservation details.");

                log("GET RESERVATION task completed.");

                return details;
            }
        }

        log("Reservation not found.");

        return "Reservation "
                + reservationId
                + " not found.";
    }

    // =========================================================
    // GET RESERVATION PORT
    // =========================================================

    @Override
    public String getReservationPort(
            String reservationId)
            throws RemoteException {

        log("GET RESERVATION PORT request received.");

        log("Reservation ID: "
                + reservationId);

        log("Searching assigned port...");

        simulateProcessing(400);

        synchronized (this) {
            String portId =
                    reservationPorts.get(
                            reservationId
                    );

            if (portId == null) {

                log("No port associated with reservation.");

                return "NONE";
            }

            log("Reservation is assigned to port: "
                    + portId);

            log("GET RESERVATION PORT task completed.");

            return portId;
        }
    }

    // =========================================================
    // MAIN
    // =========================================================

    public static void main(String[] args) {

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
            // Create RMI registry
            // -------------------------------------------------

            LocateRegistry.createRegistry(1235);

            // -------------------------------------------------
            // Create server
            // -------------------------------------------------

            ReservationServer server =
                    new ReservationServer(
                            chargingStation
                    );

            // -------------------------------------------------
            // Bind server
            // -------------------------------------------------

            Naming.rebind(
                    "rmi://localhost:1235/ReservationService",
                    server
            );

            // -------------------------------------------------
            // Server information
            // -------------------------------------------------

            System.out.println(
                    "================================="
            );

            System.out.println(
                    "   RESERVATION RMI SERVER"
            );

            System.out.println(
                    "================================="
            );

            System.out.println(
                    "Server started successfully."
            );

            System.out.println(
                    "Port: 1235"
            );

            System.out.println(
                    "Service: ReservationService"
            );

            System.out.println(
                    "Connected to ChargingStationServer "
                    + "on port 1234"
            );

            System.out.println(
                    "---------------------------------"
            );

            System.out.println(
                    "Concurrent reservation processing: ENABLED"
            );

            System.out.println(
                    "Port synchronization: "
                    + "ChargingStationServer"
            );

            System.out.println(
                    "Simulated processing delays: ENABLED"
            );

            System.out.println(
                    "Waiting for reservation requests..."
            );

            System.out.println(
                    "================================="
            );

        } catch (Exception e) {

            System.out.println(
                    "Server Error: "
                    + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}