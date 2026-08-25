import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.util.HashMap;

// RMI Server class that implements PaymentInterface
public class PaymentServer
        extends UnicastRemoteObject
        implements PaymentInterface {

    // Stores payment ID -> payment status
    private HashMap<String, String> paymentStatus;

    // Stores payment ID -> detailed payment summary
    private HashMap<String, String> paymentDetails;

    // Sequential counter for payment IDs
    private int paymentCounter = 1001;

    // Single station used by this project
    private static final String STATION_ID = "S01";

    // Remote references
    private ChargingSessionInterface chargingSession;
    private PricingInterface pricing;
    private ChargingStationInterface chargingStation;

    // Constructor with ChargingStationInterface
    public PaymentServer(
            ChargingSessionInterface chargingSession,
            PricingInterface pricing,
            ChargingStationInterface chargingStation)
            throws RemoteException {

        super(2237);

        paymentStatus =
                new HashMap<String, String>();

        paymentDetails =
                new HashMap<String, String>();

        this.chargingSession =
                chargingSession;

        this.pricing =
                pricing;

        this.chargingStation =
                chargingStation;
    }

    // Overloaded constructor for backwards compatibility
    public PaymentServer(
            ChargingSessionInterface chargingSession,
            PricingInterface pricing)
            throws RemoteException {

        this(chargingSession, pricing, null);
    }

    // =========================================================
    // THREAD LOGGING
    // =========================================================

    private void log(String message)
    {
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

    private void simulateProcessing(long milliseconds)
    {
        try
        {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();

            log("Thread interrupted during processing.");
        }
    }

    // =========================================================
    // MAKE PAYMENT
    // =========================================================

    public String makePayment(
            String sessionId)
            throws RemoteException {

        log("MAKE PAYMENT request received for Session: "
                + sessionId);

        // -----------------------------------------------------
        // Validate session ID
        // -----------------------------------------------------

        log("Validating Session ID...");

        simulateProcessing(500);

        if (sessionId == null ||
                sessionId.length() == 0) {

            log("Payment failed: Invalid Session ID.");

            return "Invalid Session ID.";
        }

        // -----------------------------------------------------
        // STEP 1: Get session status
        // -----------------------------------------------------

        log("Contacting ChargingSessionServer "
                + "to verify session.");

        simulateProcessing(500);

        String sessionInfo;

        try {

            sessionInfo =
                    chargingSession.getSessionStatus(
                            sessionId
                    );

            log("ChargingSessionServer response received.");

        }
        catch (RemoteException e) {

            log("ChargingSessionServer is unavailable.");

            return "Payment failed: "
                    + "ChargingSessionServer is unavailable ("
                    + e.getMessage()
                    + ").";
        }

        // -----------------------------------------------------
        // Validate session
        // -----------------------------------------------------

        log("Checking session status...");

        simulateProcessing(400);

        if (sessionInfo == null ||
                sessionInfo.contains("Session not found")) {

            log("Session not found: "
                    + sessionId);

            return "Payment failed: Session "
                    + sessionId
                    + " not found.";
        }

        if (!sessionInfo.contains("COMPLETED")) {

            log("Session has not completed charging.");

            return "Payment failed: Session "
                    + sessionId
                    + " has not finished charging yet.";
        }

        log("Session verified and charging is COMPLETED.");

        // -----------------------------------------------------
        // STEP 2: Get energy consumed
        // -----------------------------------------------------

        log("Requesting energy consumption "
                + "from ChargingSessionServer.");

        simulateProcessing(500);

        double energy;

        try {

            energy =
                    chargingSession.getEnergyConsumed(
                            sessionId
                    );

            log("Energy received: "
                    + energy + " kWh");

        }
        catch (RemoteException e) {

            log("Could not retrieve energy consumed.");

            return "Payment failed: "
                    + "could not retrieve energy consumed ("
                    + e.getMessage()
                    + ").";
        }

        if (energy < 0) {

            log("Invalid energy value received.");

            return "Payment failed: "
                    + "no energy data found for session "
                    + sessionId
                    + ".";
        }

        // -----------------------------------------------------
        // STEP 3: Contact PricingServer
        // -----------------------------------------------------

        log("Contacting PricingServer "
                + "to calculate final price.");

        simulateProcessing(500);

        double amount;

        try {

            amount =
                    pricing.calculatePrice(
                            STATION_ID,
                            energy
                    );

            log("PricingServer returned amount: "
                    + "Rs. " + amount);

        }
        catch (RemoteException e) {

            log("PricingServer is unavailable.");

            return "Payment failed: "
                    + "PricingServer is unavailable ("
                    + e.getMessage()
                    + ").";
        }

        if (amount < 0) {

            log("PricingServer returned invalid price.");

            return "Payment failed: "
                    + "PricingServer returned "
                    + "an invalid price.";
        }

        // -----------------------------------------------------
        // STEP 4: Generate payment ID
        // -----------------------------------------------------

        log("Generating payment ID...");

        simulateProcessing(500);

        String paymentId;

        synchronized (this) {
            paymentId =
                    "PAY-" + paymentCounter++;
        }

        log("Generated Payment ID: "
                + paymentId);

        // -----------------------------------------------------
        // STEP 5: Store payment
        // -----------------------------------------------------

        log("Saving payment information...");

        simulateProcessing(500);

        String status = "SUCCESS";

        // -----------------------------------------------------
        // STEP 6: Release Charging Port (Post-Payment Release)
        // -----------------------------------------------------

        String releaseMessage = "";

        try {

            String portId = chargingSession.getSessionPort(sessionId);

            if (portId != null && !portId.equals("NONE") && chargingStation != null) {

                log("Payment SUCCESS. Requesting ChargingStationServer to release port: "
                        + portId);

                simulateProcessing(500);

                String stationResult = chargingStation.releasePort(portId);

                releaseMessage = "\nCharging Port Status: " + stationResult;

                log("ChargingStationServer release response: " + stationResult);
            }

        } catch (Exception e) {

            log("WARNING: Could not release charging port after payment: "
                    + e.getMessage());
        }

        String details =
                "Payment ID: " + paymentId
                + "\nSession ID: " + sessionId
                + "\nEnergy Consumed: " + energy + " kWh"
                + "\nAmount: Rs. " + amount
                + "\nPayment Status: " + status
                + releaseMessage;

        synchronized (this) {
            paymentStatus.put(
                    paymentId,
                    status
            );

            paymentDetails.put(
                    paymentId,
                    details
            );
        }

        log("Payment stored successfully.");

        log("Payment ID: "
                + paymentId
                + " | Amount: Rs. "
                + amount
                + " | Status: SUCCESS");

        log("MAKE PAYMENT task completed.");

        return "Payment Successful!\n"
                + details;
    }

    // =========================================================
    // GET PAYMENT STATUS
    // =========================================================

    public String getPaymentStatus(
            String paymentId)
            throws RemoteException {

        log("GET PAYMENT STATUS request received for: "
                + paymentId);

        log("Searching payment database...");

        simulateProcessing(400);

        synchronized (this) {
            if (!paymentStatus.containsKey(paymentId)) {

                log("Payment not found: "
                        + paymentId);

                return "Payment not found.";
            }

            String status =
                    paymentStatus.get(paymentId);

            log("Payment status returned: "
                    + status);

            log("GET PAYMENT STATUS task completed.");

            return "Payment ID: "
                    + paymentId
                    + "\nPayment Status: "
                    + status;
        }
    }

    // =========================================================
    // GET PAYMENT DETAILS
    // =========================================================

    public String getPaymentDetails(
            String paymentId)
            throws RemoteException {

        log("GET PAYMENT DETAILS request received for: "
                + paymentId);

        log("Searching payment database...");

        simulateProcessing(400);

        synchronized (this) {
            if (!paymentDetails.containsKey(paymentId)) {

                log("Payment details not found: "
                        + paymentId);

                return "Payment not found.";
            }

            log("Payment details retrieved successfully.");

            log("GET PAYMENT DETAILS task completed.");

            return paymentDetails.get(paymentId);
        }
    }

    // =========================================================
    // MAIN
    // =========================================================

    public static void main(String[] args) {

        final String HOST =
                "rmi://localhost:1237/PaymentServer";

        try {

            String rmiHost = System.getenv("RMI_SERVER_HOST");
            if (rmiHost != null && !rmiHost.trim().isEmpty()) {
                System.setProperty("java.rmi.server.hostname", rmiHost);
            }

            // -------------------------------------------------
            // Connect to ChargingStationServer
            // -------------------------------------------------

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
                    chargingStation =
                            (ChargingStationInterface)
                            Naming.lookup(stationUrl);
                    System.out.println("ChargingStationServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for ChargingStationServer...");
                    System.out.println("Retry " + retryCount + "/" + maxRetries + "...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            if (chargingStation == null) {

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
            // Connect to ChargingSessionServer
            // -------------------------------------------------

            String sessionUrl = System.getenv("SESSION_URL");
            if (sessionUrl == null || sessionUrl.trim().isEmpty()) {
                String sessHost = System.getenv("SESSION_HOST");
                if (sessHost == null || sessHost.trim().isEmpty()) {
                    sessHost = "localhost";
                }
                sessionUrl = "rmi://" + sessHost + ":1236/ChargingSessionServer";
            }

            ChargingSessionInterface chargingSession = null;
            retryCount = 0;

            System.out.println("Connecting to ChargingSessionServer at " + sessionUrl + "...");

            while (retryCount < maxRetries) {
                try {
                    chargingSession =
                            (ChargingSessionInterface)
                            Naming.lookup(sessionUrl);
                    System.out.println("ChargingSessionServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for ChargingSessionServer...");
                    System.out.println("Retry " + retryCount + "/" + maxRetries + "...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            if (chargingSession == null) {

                System.out.println(
                        "Could not connect to "
                        + "ChargingSessionServer."
                );

                System.out.println(
                        "Please start ChargingSessionServer "
                        + "on port 1236."
                );

                return;
            }

            // -------------------------------------------------
            // Connect to PricingServer
            // -------------------------------------------------

            String pricingUrl = System.getenv("PRICING_URL");
            if (pricingUrl == null || pricingUrl.trim().isEmpty()) {
                String prHost = System.getenv("PRICING_HOST");
                if (prHost == null || prHost.trim().isEmpty()) {
                    prHost = "localhost";
                }
                pricingUrl = "rmi://" + prHost + ":1238/PricingService";
            }

            PricingInterface pricing = null;
            retryCount = 0;

            System.out.println("Connecting to PricingServer at " + pricingUrl + "...");

            while (retryCount < maxRetries) {
                try {
                    pricing =
                            (PricingInterface)
                            Naming.lookup(pricingUrl);
                    System.out.println("PricingServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for PricingServer...");
                    System.out.println("Retry " + retryCount + "/" + maxRetries + "...");
                    try {
                        Thread.sleep(2000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }

            if (pricing == null) {

                System.out.println(
                        "Could not connect to PricingServer."
                );

                System.out.println(
                        "Please start PricingServer "
                        + "on port 1238."
                );

                return;
            }

            // -------------------------------------------------
            // Start PaymentServer
            // -------------------------------------------------

            System.out.println(
                    "Starting Payment Server.........."
            );

            PaymentServer server =
                    new PaymentServer(
                            chargingSession,
                            pricing,
                            chargingStation
                    );

            System.out.println(
                    "Payment Server Instance Created......."
            );

            // Create RMI registry on port 1237
            LocateRegistry.createRegistry(1237);

            // Bind server to registry
            Naming.bind(
                    HOST,
                    server
            );

            System.out.println(
                    "Payment Server bound to registry successfully.."
            );

            System.out.println(
                    "Payment Server Ready...."
            );

            System.out.println(
                    "Simulated processing delays: ENABLED"
            );

            System.out.println(
                    "Waiting for payment requests..."
            );

        }
        catch (Exception ex) {

            System.out.println(
                    "Exception : "
                    + ex.getMessage()
            );

            ex.printStackTrace();
        }
    }
}