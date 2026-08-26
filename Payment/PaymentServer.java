import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;
import java.util.HashMap;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.CristianClient;
import Clock.DistributedLogger;
import Clock.LamportResult;

// RMI Server class that implements PaymentInterface
public class PaymentServer
        extends UnicastRemoteObject
        implements PaymentInterface {

    private static final String SERVER_NAME = "PaymentServer";
    private final LogicalClock logicalClock = new LogicalClock();

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

        paymentStatus = new HashMap<String, String>();
        paymentDetails = new HashMap<String, String>();

        this.chargingSession = chargingSession;
        this.pricing = pricing;
        this.chargingStation = chargingStation;
    }

    public PaymentServer(
            ChargingSessionInterface chargingSession,
            PricingInterface pricing)
            throws RemoteException {

        this(chargingSession, pricing, null);
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
    // MAKE PAYMENT
    // =========================================================

    @Override
    public String makePayment(String sessionId) throws RemoteException {
        return makePayment(sessionId, 0).getData();
    }

    @Override
    public LamportResult<String> makePayment(
            String sessionId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "MAKE PAYMENT request received for Session " + sessionId + " (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(500);

        if (sessionId == null || sessionId.length() == 0) {
            log("LOCAL", "Payment failed: Invalid Session ID.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Invalid Session ID.", respL);
        }

        // STEP 1: Get session status
        long sendL1 = logicalClock.sendEvent();
        log("SEND", "Contacting ChargingSessionServer.getSessionStatus for Session " + sessionId + " (Lamport: " + sendL1 + ")");

        LamportResult<String> sessInfoRes;
        try {
            sessInfoRes = chargingSession.getSessionStatus(sessionId, sendL1);
            logicalClock.receiveEvent(sessInfoRes.getTimestamp());
            log("RECEIVE", "ChargingSessionServer getSessionStatus response received (Session Lamport: " + sessInfoRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "ChargingSessionServer is unavailable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: ChargingSessionServer is unavailable (" + e.getMessage() + ").", respL);
        }

        String sessionInfo = sessInfoRes.getData();
        if (sessionInfo == null || sessionInfo.contains("Session not found")) {
            log("LOCAL", "Session not found: " + sessionId);
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: Session " + sessionId + " not found.", respL);
        }

        if (!sessionInfo.contains("COMPLETED")) {
            log("LOCAL", "Session has not completed charging.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: Session " + sessionId + " has not finished charging yet.", respL);
        }

        // STEP 2: Get energy consumed
        long sendL2 = logicalClock.sendEvent();
        log("SEND", "Contacting ChargingSessionServer.getEnergyConsumed for Session " + sessionId + " (Lamport: " + sendL2 + ")");

        LamportResult<Double> energyRes;
        try {
            energyRes = chargingSession.getEnergyConsumed(sessionId, sendL2);
            logicalClock.receiveEvent(energyRes.getTimestamp());
            log("RECEIVE", "ChargingSessionServer getEnergyConsumed response: " + energyRes.getData() + " kWh (Session Lamport: " + energyRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "Could not retrieve energy consumed.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: could not retrieve energy consumed (" + e.getMessage() + ").", respL);
        }

        double energy = energyRes.getData();
        if (energy < 0) {
            log("LOCAL", "Invalid energy value received.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: no energy data found for session " + sessionId + ".", respL);
        }

        // STEP 3: Contact PricingServer
        long sendL3 = logicalClock.sendEvent();
        log("SEND", "Contacting PricingServer.calculatePrice for Station " + STATION_ID + ", Energy " + energy + " kWh (Lamport: " + sendL3 + ")");

        LamportResult<Double> priceRes;
        try {
            priceRes = pricing.calculatePrice(STATION_ID, energy, sendL3);
            logicalClock.receiveEvent(priceRes.getTimestamp());
            log("RECEIVE", "PricingServer calculatePrice response: Rs. " + priceRes.getData() + " (Pricing Lamport: " + priceRes.getTimestamp() + ")");
        } catch (RemoteException e) {
            log("LOCAL", "PricingServer is unavailable.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: PricingServer is unavailable (" + e.getMessage() + ").", respL);
        }

        double amount = priceRes.getData();
        if (amount < 0) {
            log("LOCAL", "PricingServer returned invalid price.");
            long respL = logicalClock.sendEvent();
            return new LamportResult<>("Payment failed: PricingServer returned an invalid price.", respL);
        }

        // STEP 4: Generate payment ID
        String paymentId;
        synchronized (this) {
            paymentId = "PAY-" + paymentCounter++;
        }

        String status = "SUCCESS";
        String releaseMessage = "";

        // STEP 5: Post-Payment Port Release
        try {
            long sendL4 = logicalClock.sendEvent();
            log("SEND", "Contacting ChargingSessionServer.getSessionPort for Session " + sessionId + " (Lamport: " + sendL4 + ")");

            LamportResult<String> portRes = chargingSession.getSessionPort(sessionId, sendL4);
            logicalClock.receiveEvent(portRes.getTimestamp());
            log("RECEIVE", "ChargingSessionServer getSessionPort response: " + portRes.getData() + " (Session Lamport: " + portRes.getTimestamp() + ")");

            String portId = portRes.getData();
            if (portId != null && !portId.equals("NONE") && chargingStation != null) {
                long sendL5 = logicalClock.sendEvent();
                log("SEND", "Payment SUCCESS. Contacting ChargingStationServer.releasePort for Port " + portId + " (Lamport: " + sendL5 + ")");

                LamportResult<String> releaseRes = chargingStation.releasePort(portId, sendL5);
                logicalClock.receiveEvent(releaseRes.getTimestamp());
                log("RECEIVE", "ChargingStationServer releasePort response: " + releaseRes.getData() + " (Station Lamport: " + releaseRes.getTimestamp() + ")");

                releaseMessage = "\nCharging Port Status: " + releaseRes.getData();
            }
        } catch (Exception e) {
            log("LOCAL", "WARNING: Could not release charging port after payment: " + e.getMessage());
        }

        String details =
                "Payment ID: " + paymentId
                + "\nSession ID: " + sessionId
                + "\nEnergy Consumed: " + energy + " kWh"
                + "\nAmount: Rs. " + amount
                + "\nPayment Status: " + status
                + releaseMessage;

        synchronized (this) {
            paymentStatus.put(paymentId, status);
            paymentDetails.put(paymentId, details);
            logicalClock.tick();
            log("LOCAL", "Payment " + paymentId + " stored successfully.");
        }

        String result = "Payment Successful!\n" + details;
        long respL = logicalClock.sendEvent();
        log("SEND", "Returning MAKE PAYMENT response to client (Lamport: " + respL + ")");

        return new LamportResult<>(result, respL);
    }

    // =========================================================
    // GET PAYMENT STATUS
    // =========================================================

    @Override
    public String getPaymentStatus(String paymentId) throws RemoteException {
        return getPaymentStatus(paymentId, 0).getData();
    }

    @Override
    public LamportResult<String> getPaymentStatus(
            String paymentId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET PAYMENT STATUS request for " + paymentId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            if (!paymentStatus.containsKey(paymentId)) {
                log("LOCAL", "Payment not found: " + paymentId);
                long respL = logicalClock.sendEvent();
                return new LamportResult<>("Payment not found.", respL);
            }

            String status = paymentStatus.get(paymentId);
            logicalClock.tick();
            log("LOCAL", "Payment ID " + paymentId + " status: " + status);

            String result = "Payment ID: " + paymentId + "\nPayment Status: " + status;
            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET PAYMENT STATUS response (Lamport: " + respL + ")");
            return new LamportResult<>(result, respL);
        }
    }

    // =========================================================
    // GET PAYMENT DETAILS
    // =========================================================

    @Override
    public String getPaymentDetails(String paymentId) throws RemoteException {
        return getPaymentDetails(paymentId, 0).getData();
    }

    @Override
    public LamportResult<String> getPaymentDetails(
            String paymentId,
            long clientLamport)
            throws RemoteException {

        long recvL = logicalClock.receiveEvent(clientLamport);
        log("RECEIVE", "GET PAYMENT DETAILS request for " + paymentId + " received (Client Lamport: " + clientLamport + "). Clock updated to " + recvL);

        simulateProcessing(400);

        synchronized (this) {
            if (!paymentDetails.containsKey(paymentId)) {
                log("LOCAL", "Payment details not found: " + paymentId);
                long respL = logicalClock.sendEvent();
                return new LamportResult<>("Payment not found.", respL);
            }

            String details = paymentDetails.get(paymentId);
            logicalClock.tick();
            log("LOCAL", "Payment details retrieved for " + paymentId);

            long respL = logicalClock.sendEvent();
            log("SEND", "Returning GET PAYMENT DETAILS response (Lamport: " + respL + ")");
            return new LamportResult<>(details, respL);
        }
    }

    // =========================================================
    // MAIN
    // =========================================================

    public static void main(String[] args) {
        final String HOST = "rmi://localhost:1237/PaymentServer";

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
                    chargingSession = (ChargingSessionInterface) Naming.lookup(sessionUrl);
                    System.out.println("ChargingSessionServer connected.");
                    break;
                } catch (Exception e) {
                    retryCount++;
                    System.out.println("Waiting for ChargingSessionServer... Retry " + retryCount + "/" + maxRetries + "...");
                    try { Thread.sleep(2000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); break; }
                }
            }

            if (chargingSession == null) {
                System.out.println("Could not connect to ChargingSessionServer.");
                return;
            }

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

            PaymentServer server = new PaymentServer(chargingSession, pricing, chargingStation);

            LocateRegistry.createRegistry(1237);

            Naming.bind(HOST, server);

            System.out.println("Payment Server bound to registry successfully.");

            try {
                server.synchronizeClock();
            } catch (Exception syncEx) {
                System.out.println("Startup clock synchronization warning: " + syncEx.getMessage());
            }

            System.out.println("Waiting for payment requests...");

        } catch (Exception ex) {
            System.out.println("Exception: " + ex.getMessage());
            ex.printStackTrace();
        }
    }
}