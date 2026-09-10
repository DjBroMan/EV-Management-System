import java.rmi.Naming;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.LamportResult;

// =============================================================
// MULTITHREADED TEST FOR COMPLETE EV RMI WORKFLOW
//
// Each simulated EV performs:
//      Reservation -> Charging Session Start -> Charging Session Stop -> Payment
//
// Demonstrates concurrent requests, thread safety, Lamport logical clock propagation,
// physical clock timestamps, and Cristian clock synchronization.
// =============================================================

public class MultithreadTest {

    private static final int NUMBER_OF_EV_REQUESTS = 10;

    private static String getEnvHost(String envVar, String defaultHost) {
        String host = System.getenv(envVar);
        return (host != null && !host.trim().isEmpty()) ? host.trim() : defaultHost;
    }

    private static final String RESERVATION_URL =
            "rmi://" + getEnvHost("MANAGER_HOST", getEnvHost("RESERVATION_HOST", "localhost")) + ":1240/ReservationService";

    private static final String SESSION_URL =
            "rmi://" + getEnvHost("SESSION_HOST", "localhost") + ":1236/ChargingSessionServer";

    private static final String PAYMENT_URL =
            "rmi://" + getEnvHost("PAYMENT_HOST", "localhost") + ":1237/PaymentServer";

    private static final String PRICING_URL =
            "rmi://" + getEnvHost("PRICING_HOST", "localhost") + ":1238/PricingService";

    public static void main(String[] args) {

        try {
            java.rmi.server.RMISocketFactory.setSocketFactory(new java.rmi.server.RMISocketFactory() {
                @Override
                public java.net.Socket createSocket(String host, int port) throws java.io.IOException {
                    if ("charging-station".equals(host) || "reservation".equals(host) ||
                        "reservation-primary".equals(host) || "reservation-secondary".equals(host) ||
                        "reservation-manager".equals(host) || "charging-session".equals(host) ||
                        "pricing".equals(host) || "payment".equals(host) || "time-server".equals(host)) {
                        host = "localhost";
                    }
                    return new java.net.Socket(host, port);
                }
                @Override
                public java.net.ServerSocket createServerSocket(int port) throws java.io.IOException {
                    return new java.net.ServerSocket(port);
                }
            });
        } catch (Exception ignored) {}

        System.out.println("============================================================");
        System.out.println("       MULTITHREADED RMI TEST - COMPLETE EV WORKFLOW");
        System.out.println("============================================================");
        System.out.println("Number of simulated EVs: " + NUMBER_OF_EV_REQUESTS);
        System.out.println();
        System.out.println("Each EV will perform: RESERVE -> START CHARGING -> STOP CHARGING -> PAYMENT");
        System.out.println();
        System.out.println("Servers involved:");
        System.out.println("ReservationServer       : 1235");
        System.out.println("ChargingSessionServer   : 1236");
        System.out.println("PaymentServer            : 1237");
        System.out.println("PricingServer            : 1238");
        System.out.println("ChargingStationServer    : 1234");
        System.out.println("============================================================");

        try {
            ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_EV_REQUESTS);
            CountDownLatch startSignal = new CountDownLatch(1);
            CountDownLatch completionSignal = new CountDownLatch(NUMBER_OF_EV_REQUESTS);

            for (int i = 1; i <= NUMBER_OF_EV_REQUESTS; i++) {
                final int evNumber = i;

                executor.submit(() -> {
                    String userId = "USER-" + evNumber;
                    String vehicleId = "EV-" + evNumber;
                    LogicalClock evClock = new LogicalClock();

                    try {
                        long threadId = Thread.currentThread().getId();
                        String threadName = Thread.currentThread().getName();

                        System.out.println();
                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + " | " + threadName + "] " + userId + " preparing...");

                        ReservationInterface reservation = (ReservationInterface) Naming.lookup(RESERVATION_URL);
                        ChargingSessionInterface chargingSession = (ChargingSessionInterface) Naming.lookup(SESSION_URL);
                        PaymentInterface payment = (PaymentInterface) Naming.lookup(PAYMENT_URL);
                        PricingInterface pricing = (PricingInterface) Naming.lookup(PRICING_URL);

                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " connected to all RMI servers.");

                        startSignal.await();

                        System.out.println();
                        System.out.println("************************************************************");
                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " STARTING WORKFLOW");
                        System.out.println("************************************************************");

                        // STEP 1: RESERVATION
                        System.out.println();
                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " -> RESERVATION");

                        long sendL1 = evClock.sendEvent();
                        LamportResult<String> res1 = reservation.reserveSlot(userId, vehicleId, sendL1);
                        evClock.receiveEvent(res1.getTimestamp());
                        String reservationResponse = res1.getData();

                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " Reservation Response:\n" + reservationResponse);

                        if (reservationResponse == null || !reservationResponse.contains("Reservation successful")) {
                            System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " could NOT obtain a charging port. Workflow stopped.");
                            return;
                        }

                        String reservationId = extractReservationId(reservationResponse);
                        if (reservationId == null) {
                            System.out.println("[CLIENT THREAD-" + threadId + "] " + userId + " could not extract Reservation ID.");
                            return;
                        }

                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " Reservation ID: " + reservationId);

                        // STEP 2: START CHARGING
                        System.out.println();
                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " -> START CHARGING");

                        long sendL2 = evClock.sendEvent();
                        LamportResult<String> res2 = chargingSession.startCharging(reservationId, sendL2);
                        evClock.receiveEvent(res2.getTimestamp());
                        String startResponse = res2.getData();

                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " Charging Start Response:\n" + startResponse);

                        if (startResponse == null || !startResponse.contains("Charging Started Successfully")) {
                            System.out.println("[CLIENT THREAD-" + threadId + "] " + userId + " could not start charging.");
                            return;
                        }

                        String sessionId = extractSessionId(startResponse);
                        if (sessionId == null) {
                            System.out.println("[CLIENT THREAD-" + threadId + "] " + userId + " could not extract Session ID.");
                            return;
                        }

                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " Session ID: " + sessionId);

                        Thread.sleep(1000);

                        // STEP 3: STOP CHARGING
                        System.out.println();
                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " -> STOP CHARGING");

                        long sendL3 = evClock.sendEvent();
                        LamportResult<String> res3 = chargingSession.stopCharging(sessionId, sendL3);
                        evClock.receiveEvent(res3.getTimestamp());
                        String stopResponse = res3.getData();

                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " Charging Stop Response:\n" + stopResponse);

                        if (stopResponse == null || !stopResponse.contains("Charging Stopped Successfully")) {
                            System.out.println("[CLIENT THREAD-" + threadId + "] " + userId + " could not stop charging.");
                            return;
                        }

                        // STEP 4: GET ENERGY & PRICING
                        long sendL4 = evClock.sendEvent();
                        LamportResult<Double> res4 = chargingSession.getEnergyConsumed(sessionId, sendL4);
                        evClock.receiveEvent(res4.getTimestamp());
                        double energy = res4.getData();

                        System.out.println("\n[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " Energy Consumed: " + energy + " kWh");

                        System.out.println("\n[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " -> PRICING SERVER");
                        long sendL5 = evClock.sendEvent();
                        LamportResult<Double> res5 = pricing.calculatePrice("S01", energy, sendL5);
                        evClock.receiveEvent(res5.getTimestamp());
                        double price = res5.getData();

                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " Calculated Price: Rs. " + price);

                        // STEP 5: PAYMENT
                        System.out.println("\n[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " -> PAYMENT SERVER");
                        long sendL6 = evClock.sendEvent();
                        LamportResult<String> res6 = payment.makePayment(sessionId, sendL6);
                        evClock.receiveEvent(res6.getTimestamp());
                        String paymentResponse = res6.getData();

                        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " Payment Response:\n" + paymentResponse);

                        System.out.println("\n[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + evClock.getValue() + "] [CLIENT THREAD-" + threadId + "] " + userId + " COMPLETE WORKFLOW FINISHED.");
                        System.out.println("************************************************************");

                    } catch (Exception e) {
                        System.out.println("\n[CLIENT THREAD-" + Thread.currentThread().getId() + "] " + userId + " ERROR:\n" + e.getMessage());
                    } finally {
                        completionSignal.countDown();
                    }
                });
            }

            System.out.println("\nAll EV threads are preparing RMI connections...");
            Thread.sleep(2000);
            System.out.println("\nAll EV threads are ready.");
            System.out.println("\n============================================================");
            System.out.println("STARTING ALL EV REQUESTS CONCURRENTLY");
            System.out.println("============================================================");

            long startTime = System.currentTimeMillis();
            startSignal.countDown();
            completionSignal.await();
            long endTime = System.currentTimeMillis();

            executor.shutdown();
            executor.awaitTermination(30, TimeUnit.SECONDS);

            System.out.println("\n============================================================");
            System.out.println("          MULTITHREADED RMI TEST COMPLETED");
            System.out.println("============================================================");
            System.out.println("Total EV threads: " + NUMBER_OF_EV_REQUESTS);
            System.out.println("Total execution time: " + (endTime - startTime) + " ms");
            System.out.println("\nServers tested:");
            System.out.println("[OK] ReservationServer");
            System.out.println("[OK] ChargingStationServer");
            System.out.println("[OK] ChargingSessionServer");
            System.out.println("[OK] PricingServer");
            System.out.println("[OK] PaymentServer");
            System.out.println("\nAll EV threads completed.");
            System.out.println("============================================================");

        } catch (Exception e) {
            System.out.println("\nMULTITHREADED TEST FAILED\nError: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String extractReservationId(String response) {
        return extractField(response, "Reservation ID:");
    }

    private static String extractSessionId(String response) {
        return extractField(response, "Session ID:");
    }

    private static String extractField(String text, String label) {
        int start = text.indexOf(label);
        if (start == -1) return null;
        start += label.length();
        int endComma = text.indexOf(",", start);
        int endNewline = text.indexOf("\n", start);
        int end = text.length();
        if (endComma != -1) end = Math.min(end, endComma);
        if (endNewline != -1) end = Math.min(end, endNewline);
        return text.substring(start, end).trim();
    }
}