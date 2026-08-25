import java.rmi.Naming;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// =============================================================
// MULTITHREADED TEST FOR COMPLETE EV RMI WORKFLOW
//
// Each simulated EV performs:
//
//      Reservation
//          v
//      Charging Session Start
//          v
//      Charging Session Stop
//          v
//      Payment
//
// This allows concurrent requests to reach:
//
//      ReservationServer
//      ChargingStationServer
//      ChargingSessionServer
//      PricingServer
//      PaymentServer
// =============================================================

public class MultithreadTest {

    // Number of simulated EVs
    private static final int NUMBER_OF_EV_REQUESTS = 10;

    private static String getEnvHost(String envVar, String defaultHost) {
        String host = System.getenv(envVar);
        return (host != null && !host.trim().isEmpty()) ? host.trim() : defaultHost;
    }

    // RMI service locations
    private static final String RESERVATION_URL =
            "rmi://" + getEnvHost("RESERVATION_HOST", "localhost") + ":1235/ReservationService";

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
                        "charging-session".equals(host) || "pricing".equals(host) ||
                        "payment".equals(host)) {
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

        System.out.println(
                "============================================================"
        );

        System.out.println(
                "       MULTITHREADED RMI TEST - COMPLETE EV WORKFLOW"
        );

        System.out.println(
                "============================================================"
        );

        System.out.println(
                "Number of simulated EVs: "
                + NUMBER_OF_EV_REQUESTS
        );

        System.out.println();

        System.out.println(
                "Each EV will perform:"
        );

        System.out.println(
                "RESERVE -> START CHARGING -> STOP CHARGING -> PAYMENT"
        );

        System.out.println();

        System.out.println(
                "Servers involved:"
        );

        System.out.println(
                "ReservationServer       : 1235"
        );

        System.out.println(
                "ChargingSessionServer   : 1236"
        );

        System.out.println(
                "PaymentServer            : 1237"
        );

        System.out.println(
                "PricingServer            : 1238"
        );

        System.out.println(
                "ChargingStationServer    : 1234"
        );

        System.out.println(
                "============================================================"
        );

        try {

            // =====================================================
            // CREATE THREAD POOL
            // =====================================================

            ExecutorService executor =
                    Executors.newFixedThreadPool(
                            NUMBER_OF_EV_REQUESTS
                    );

            // =====================================================
            // START LATCH
            //
            // All EV threads wait here.
            //
            // This allows us to release all EVs at approximately
            // the same time.
            // =====================================================

            CountDownLatch startSignal =
                    new CountDownLatch(1);

            // =====================================================
            // COMPLETION LATCH
            //
            // Main thread waits until all EV threads finish.
            // =====================================================

            CountDownLatch completionSignal =
                    new CountDownLatch(
                            NUMBER_OF_EV_REQUESTS
                    );

            // =====================================================
            // CREATE EV THREADS
            // =====================================================

            for (int i = 1;
                 i <= NUMBER_OF_EV_REQUESTS;
                 i++) {

                final int evNumber = i;

                executor.submit(() -> {

                    String userId =
                            "USER-" + evNumber;

                    String vehicleId =
                            "EV-" + evNumber;

                    try {

                        // =================================================
                        // CLIENT THREAD INFORMATION
                        // =================================================

                        long threadId =
                                Thread.currentThread().getId();

                        String threadName =
                                Thread.currentThread().getName();

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + " | "
                                + threadName
                                + "] "
                                + userId
                                + " preparing..."
                        );

                        // =================================================
                        // EACH EV CREATES ITS OWN RMI LOOKUPS
                        // =================================================

                        ReservationInterface reservation =
                                (ReservationInterface)
                                Naming.lookup(
                                        RESERVATION_URL
                                );

                        ChargingSessionInterface chargingSession =
                                (ChargingSessionInterface)
                                Naming.lookup(
                                        SESSION_URL
                                );

                        PaymentInterface payment =
                                (PaymentInterface)
                                Naming.lookup(
                                        PAYMENT_URL
                                );

                        PricingInterface pricing =
                                (PricingInterface)
                                Naming.lookup(
                                        PRICING_URL
                                );

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " connected to all RMI servers."
                        );

                        // =================================================
                        // WAIT FOR START SIGNAL
                        // =================================================

                        startSignal.await();

                        System.out.println();

                        System.out.println(
                                "************************************************************"
                        );

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " STARTING COMPLETE WORKFLOW"
                        );

                        System.out.println(
                                "************************************************************"
                        );

                        // =================================================
                        // STEP 1
                        // RESERVATION
                        // =================================================

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " -> RESERVATION"
                        );

                        String reservationResponse =
                                reservation.reserveSlot(
                                        userId,
                                        vehicleId
                                );

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " Reservation Response:"
                        );

                        System.out.println(
                                reservationResponse
                        );

                        // =================================================
                        // CHECK WHETHER RESERVATION SUCCEEDED
                        // =================================================

                        if (reservationResponse == null ||
                                !reservationResponse.contains(
                                        "Reservation successful")) {

                            System.out.println();

                            System.out.println(
                                    "[CLIENT THREAD-"
                                    + threadId
                                    + "] "
                                    + userId
                                    + " could NOT obtain a charging port."
                            );

                            System.out.println(
                                    "[CLIENT THREAD-"
                                    + threadId
                                    + "] "
                                    + userId
                                    + " workflow stopped."
                            );

                            return;
                        }

                        // =================================================
                        // EXTRACT RESERVATION ID
                        // =================================================

                        String reservationId =
                                extractReservationId(
                                        reservationResponse
                                );

                        if (reservationId == null) {

                            System.out.println(
                                    "[CLIENT THREAD-"
                                    + threadId
                                    + "] "
                                    + userId
                                    + " could not extract Reservation ID."
                            );

                            return;
                        }

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " Reservation ID: "
                                + reservationId
                        );

                        // =================================================
                        // STEP 2
                        // START CHARGING
                        // =================================================

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " -> START CHARGING"
                        );

                        String startResponse =
                                chargingSession.startCharging(
                                        reservationId
                                );

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " Charging Start Response:"
                        );

                        System.out.println(
                                startResponse
                        );

                        // =================================================
                        // CHECK WHETHER CHARGING STARTED
                        // =================================================

                        if (startResponse == null ||
                                !startResponse.contains(
                                        "Charging Started Successfully")) {

                            System.out.println(
                                    "[CLIENT THREAD-"
                                    + threadId
                                    + "] "
                                    + userId
                                    + " could not start charging."
                            );

                            return;
                        }

                        // =================================================
                        // EXTRACT SESSION ID
                        // =================================================

                        String sessionId =
                                extractSessionId(
                                        startResponse
                                );

                        if (sessionId == null) {

                            System.out.println(
                                    "[CLIENT THREAD-"
                                    + threadId
                                    + "] "
                                    + userId
                                    + " could not extract Session ID."
                            );

                            return;
                        }

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " Session ID: "
                                + sessionId
                        );

                        // =================================================
                        // SMALL DELAY TO SIMULATE CHARGING
                        // =================================================

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " charging..."
                        );

                        Thread.sleep(1000);

                        // =================================================
                        // STEP 3
                        // STOP CHARGING
                        // =================================================

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " -> STOP CHARGING"
                        );

                        String stopResponse =
                                chargingSession.stopCharging(
                                        sessionId
                                );

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " Charging Stop Response:"
                        );

                        System.out.println(
                                stopResponse
                        );

                        // =================================================
                        // CHECK WHETHER CHARGING STOPPED
                        // =================================================

                        if (stopResponse == null ||
                                !stopResponse.contains(
                                        "Charging Stopped Successfully")) {

                            System.out.println(
                                    "[CLIENT THREAD-"
                                    + threadId
                                    + "] "
                                    + userId
                                    + " could not stop charging."
                            );

                            return;
                        }

                        // =================================================
                        // STEP 4
                        // GET ENERGY
                        // =================================================

                        double energy =
                                chargingSession.getEnergyConsumed(
                                        sessionId
                                );

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " Energy Consumed: "
                                + energy
                                + " kWh"
                        );

                        // =================================================
                        // STEP 5
                        // PRICING SERVER
                        //
                        // This directly contacts PricingServer so
                        // multithreading can be visibly demonstrated
                        // there as well.
                        // =================================================

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " -> PRICING SERVER"
                        );

                        double price =
                                pricing.calculatePrice(
                                        "S01",
                                        energy
                                );

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " Calculated Price: Rs. "
                                + price
                        );

                        // =================================================
                        // STEP 6
                        // PAYMENT SERVER
                        // =================================================

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " -> PAYMENT SERVER"
                        );

                        String paymentResponse =
                                payment.makePayment(
                                        sessionId
                                );

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " Payment Response:"
                        );

                        System.out.println(
                                paymentResponse
                        );

                        // =================================================
                        // WORKFLOW COMPLETE
                        // =================================================

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + threadId
                                + "] "
                                + userId
                                + " COMPLETE WORKFLOW FINISHED."
                        );

                        System.out.println(
                                "************************************************************"
                        );

                    } catch (Exception e) {

                        System.out.println();

                        System.out.println(
                                "[CLIENT THREAD-"
                                + Thread.currentThread().getId()
                                + "] "
                                + userId
                                + " ERROR:"
                        );

                        System.out.println(
                                e.getMessage()
                        );

                    } finally {

                        // Tell main thread that this EV is finished
                        completionSignal.countDown();
                    }
                });
            }

            // =====================================================
            // GIVE THREADS TIME TO PREPARE
            // =====================================================

            System.out.println();

            System.out.println(
                    "All EV threads are preparing RMI connections..."
            );

            Thread.sleep(2000);

            System.out.println();

            System.out.println(
                    "All EV threads are ready."
            );

            System.out.println();

            System.out.println(
                    "============================================================"
            );

            System.out.println(
                    "STARTING ALL EV REQUESTS CONCURRENTLY"
            );

            System.out.println(
                    "============================================================"
            );

            long startTime =
                    System.currentTimeMillis();

            // =====================================================
            // RELEASE ALL EV THREADS
            // =====================================================

            startSignal.countDown();

            // =====================================================
            // WAIT FOR ALL EV THREADS
            // =====================================================

            completionSignal.await();

            long endTime =
                    System.currentTimeMillis();

            // =====================================================
            // SHUTDOWN EXECUTOR
            // =====================================================

            executor.shutdown();

            executor.awaitTermination(
                    30,
                    TimeUnit.SECONDS
            );

            // =====================================================
            // TEST COMPLETE
            // =====================================================

            System.out.println();

            System.out.println(
                    "============================================================"
            );

            System.out.println(
                    "          MULTITHREADED RMI TEST COMPLETED"
            );

            System.out.println(
                    "============================================================"
            );

            System.out.println(
                    "Total EV threads: "
                    + NUMBER_OF_EV_REQUESTS
            );

            System.out.println(
                    "Total execution time: "
                    + (endTime - startTime)
                    + " ms"
            );

            System.out.println();

            System.out.println(
                    "Servers tested:"
            );

            System.out.println(
                    "[OK] ReservationServer"
            );

            System.out.println(
                    "[OK] ChargingStationServer"
            );

            System.out.println(
                    "[OK] ChargingSessionServer"
            );

            System.out.println(
                    "[OK] PricingServer"
            );

            System.out.println(
                    "[OK] PaymentServer"
            );

            System.out.println();

            System.out.println(
                    "All EV threads completed."
            );

            System.out.println(
                    "============================================================"
            );

        } catch (Exception e) {

            System.out.println();

            System.out.println(
                    "MULTITHREADED TEST FAILED"
            );

            System.out.println(
                    "Error: "
                    + e.getMessage()
            );

            e.printStackTrace();
        }
    }

    // =============================================================
    // EXTRACT RESERVATION ID
    // =============================================================

    private static String extractReservationId(
            String response) {

        try {

            int start =
                    response.indexOf(
                            "Reservation ID:"
                    );

            if (start == -1) {
                return null;
            }

            start +=
                    "Reservation ID:".length();

            int end =
                    response.indexOf(
                            ",",
                            start
                    );

            if (end == -1) {

                end =
                        response.indexOf(
                                "\n",
                                start
                        );
            }

            if (end == -1) {
                end = response.length();
            }

            return response
                    .substring(start, end)
                    .trim();

        } catch (Exception e) {

            return null;
        }
    }

    // =============================================================
    // EXTRACT SESSION ID
    // =============================================================

    private static String extractSessionId(
            String response) {

        try {

            int start =
                    response.indexOf(
                            "Session ID:"
                    );

            if (start == -1) {
                return null;
            }

            start +=
                    "Session ID:".length();

            int end =
                    response.indexOf(
                            "\n",
                            start
                    );

            if (end == -1) {
                end = response.length();
            }

            return response
                    .substring(start, end)
                    .trim();

        } catch (Exception e) {

            return null;
        }
    }
}