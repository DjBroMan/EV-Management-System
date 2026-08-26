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

// RMI Server implementation for dynamic EV charging price computation
public class PricingServer extends UnicastRemoteObject
        implements PricingInterface {

    private static final long serialVersionUID = 1L;
    private static final String SERVER_NAME = "PricingServer";
    private final LogicalClock logicalClock = new LogicalClock();

    private static final double BASE_PRICE = 10.0;
    private Map<String, String> stationDemand;

    protected PricingServer() throws RemoteException {
        super(2238);

        stationDemand = new HashMap<>();
        stationDemand.put("S01", "LOW");
        stationDemand.put("S02", "MEDIUM");
        stationDemand.put("S03", "HIGH");
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
            log("LOCAL",
                    "Clock synchronization completed successfully. Calculated offset: " + res.clockOffsetMs + " ms");
            return "Clock synchronized successfully. Offset: " + res.clockOffsetMs + " ms";
        } else {
            log("LOCAL", "Clock synchronization failed: " + res.errorMessage);
            return "Clock synchronization failed: " + res.errorMessage;
        }
    }

    // =========================================================
    // CALCULATE PRICE
    // =========================================================

    @Override
    public double calculatePrice(String stationId, double energyConsumed) throws RemoteException {
        return calculatePrice(stationId, energyConsumed, 0).getData();
    }

    @Override
    public LamportResult<Double> calculatePrice(
            String stationId,
            double energyConsumed,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "CALCULATE PRICE request received for Station " + stationId + ", Energy " + energyConsumed
                + " kWh (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(300);

        if (energyConsumed < 0) {
            log("LOCAL", "Invalid energy value.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>(-1.0, respL);
        }

        simulateProcessing(500);

        String demand = stationDemand.get(stationId);
        if (demand == null) {
            demand = "LOW";
        }

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

        simulateProcessing(500);
        double finalPrice = BASE_PRICE * energyConsumed * multiplier;

        logicalClock.tick();
        log("LOCAL", "Price calculated: " + BASE_PRICE + " * " + energyConsumed + " * " + multiplier + " = Rs. "
                + finalPrice);

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning CALCULATE PRICE response Rs. " + finalPrice + " (Lamport: " + sendL + ")");

        return new LamportResult<>(finalPrice, sendL);
    }

    // =========================================================
    // GET DEMAND MULTIPLIER
    // =========================================================

    @Override
    public double getDemandMultiplier(String stationId) throws RemoteException {
        return getDemandMultiplier(stationId, 0).getData();
    }

    @Override
    public LamportResult<Double> getDemandMultiplier(
            String stationId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET DEMAND MULTIPLIER request received for Station " + stationId + " (Client Lamport: "
                + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        String demand = stationDemand.get(stationId);
        if (demand == null) {
            demand = "LOW";
        }

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

        logicalClock.tick();
        log("LOCAL", "Demand multiplier: " + multiplier);

        long sendL = logicalClock.sendEvent();
        log("SEND", "Returning GET DEMAND MULTIPLIER response " + multiplier + " (Lamport: " + sendL + ")");

        return new LamportResult<>(multiplier, sendL);
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

            System.out.println("Starting Pricing Server...");

            LocateRegistry.createRegistry(1238);

            PricingServer server = new PricingServer();

            Naming.rebind("rmi://localhost:1238/PricingService", server);

            System.out.println("=================================");
            System.out.println("       PRICING RMI SERVER");
            System.out.println("=================================");

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Waiting for pricing requests...");
            System.out.println("=================================");

        } catch (Exception e) {
            System.out.println("Server Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}