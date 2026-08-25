import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;

// RMI Server implementation for dynamic EV charging price computation
public class PricingServer extends UnicastRemoteObject
        implements PricingInterface {

    private static final long serialVersionUID = 1L;

    // Standard rate per kWh
    private static final double BASE_PRICE = 10.0;

    // Stores station ID -> demand level
    private Map<String, String> stationDemand;

    // Constructor
    protected PricingServer() throws RemoteException {
        super(2238);

        stationDemand = new HashMap<>();

        // Sample station demand levels
        stationDemand.put("S01", "LOW");
        stationDemand.put("S02", "MEDIUM");
        stationDemand.put("S03", "HIGH");
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
    // CALCULATE PRICE
    // =========================================================

    @Override
    public double calculatePrice(
            String stationId,
            double energyConsumed)
            throws RemoteException {

        log("CALCULATE PRICE request received.");
        log("Station: " + stationId);
        log("Energy requested: "
                + energyConsumed + " kWh");

        // -----------------------------------------------------
        // Validate energy
        // -----------------------------------------------------

        log("Validating energy consumption...");

        simulateProcessing(300);

        if (energyConsumed < 0) {

            log("Invalid energy value.");

            return -1;
        }

        // -----------------------------------------------------
        // Get demand level
        // -----------------------------------------------------

        log("Checking demand level for station "
                + stationId);

        simulateProcessing(500);

        String demand =
                stationDemand.get(stationId);

        if (demand == null) {

            log("Station not found. "
                    + "Defaulting demand to LOW.");

            demand = "LOW";
        }

        log("Demand level: " + demand);

        // -----------------------------------------------------
        // Determine multiplier
        // -----------------------------------------------------

        log("Determining demand multiplier...");

        simulateProcessing(400);

        double multiplier;

        switch (demand) {

            case "LOW":

                multiplier = 1.0;

                break;

            case "MEDIUM":

                multiplier = 1.25;

                break;

            case "HIGH":

                multiplier = 1.50;

                break;

            default:

                multiplier = 1.0;
        }

        log("Base price: Rs. "
                + BASE_PRICE + "/kWh");

        log("Demand multiplier: "
                + multiplier);

        // -----------------------------------------------------
        // Calculate final price
        // -----------------------------------------------------

        log("Calculating final price...");

        simulateProcessing(500);

        double finalPrice =
                BASE_PRICE
                * energyConsumed
                * multiplier;

        log("Price calculation:");

        log(
            BASE_PRICE
            + " * "
            + energyConsumed
            + " * "
            + multiplier
        );

        log("Final price: Rs. "
                + finalPrice);

        log("CALCULATE PRICE task completed.");

        return finalPrice;
    }

    // =========================================================
    // GET DEMAND MULTIPLIER
    // =========================================================

    @Override
    public double getDemandMultiplier(
            String stationId)
            throws RemoteException {

        log("GET DEMAND MULTIPLIER request received.");
        log("Station: " + stationId);

        // -----------------------------------------------------
        // Look up demand
        // -----------------------------------------------------

        log("Looking up station demand...");

        simulateProcessing(400);

        String demand =
                stationDemand.get(stationId);

        if (demand == null) {

            log("Station not found. "
                    + "Defaulting demand to LOW.");

            demand = "LOW";
        }

        log("Demand level: " + demand);

        // -----------------------------------------------------
        // Determine multiplier
        // -----------------------------------------------------

        log("Calculating demand multiplier...");

        simulateProcessing(400);

        double multiplier;

        switch (demand) {

            case "LOW":

                multiplier = 1.0;

                break;

            case "MEDIUM":

                multiplier = 1.25;

                break;

            case "HIGH":

                multiplier = 1.50;

                break;

            default:

                multiplier = 1.0;
        }

        log("Returning multiplier: "
                + multiplier);

        log("GET DEMAND MULTIPLIER task completed.");

        return multiplier;
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

            System.out.println(
                    "Starting Pricing Server..."
            );

            // Start RMI registry on port 1238
            LocateRegistry.createRegistry(1238);

            // Create PricingServer instance
            PricingServer server =
                    new PricingServer();

            // Bind service
            Naming.rebind(
                    "rmi://localhost:1238/PricingService",
                    server
            );

            System.out.println(
                    "================================="
            );

            System.out.println(
                    "       PRICING RMI SERVER"
            );

            System.out.println(
                    "================================="
            );

            System.out.println(
                    "Server started successfully."
            );

            System.out.println(
                    "Port: 1238"
            );

            System.out.println(
                    "Service: PricingService"
            );

            System.out.println(
                    "---------------------------------"
            );

            System.out.println(
                    "Stations:"
            );

            System.out.println(
                    "S01 -> LOW demand"
            );

            System.out.println(
                    "S02 -> MEDIUM demand"
            );

            System.out.println(
                    "S03 -> HIGH demand"
            );

            System.out.println(
                    "---------------------------------"
            );

            System.out.println(
                    "Simulated processing delays: ENABLED"
            );

            System.out.println(
                    "Waiting for pricing requests..."
            );

            System.out.println(
                    "================================="
            );

        }
        catch (Exception e) {

            System.out.println(
                    "Server Error: "
                    + e.getMessage()
            );

            e.printStackTrace();
        }
    }
}