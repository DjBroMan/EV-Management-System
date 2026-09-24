import java.rmi.Naming;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.LamportResult;

/**
 * Standalone demo driving the Wallet cross-server Lamport clock scenario:
 *
 *   Wallet top-up -> Charging starts -> Charging requests balance
 *   -> Payment processes deduction -> Charging receives result
 *   -> Charging continues OR stops because balance is exhausted
 *
 * This program is the CLIENT-SIDE narrator: it drives the scenario and
 * prints each of its own RMI calls with Physical + Lamport timestamps.
 * The AUTHORITATIVE proof of cross-server Lamport propagation is the
 * server-side log output of the charging-session-* and payment-* containers
 * themselves (via DistributedLogger) -- watch those with, e.g.:
 *
 *   docker compose logs charging-session-1 payment-1 -f
 *
 * while this program runs. See demo.md Step 13 and docs/WALLET.md.
 */
public class WalletDemo {

    private static final LogicalClock clock = new LogicalClock();
    // Unique per run so a leftover still-charging session from a previous
    // demo run (billed against the same user) can't mask this run's balance
    // exhaustion -- each run gets its own clean wallet.
    private static final String USER_ID = "WALLET-DEMO-USER-" + System.currentTimeMillis();
    private static final String VEHICLE_ID = "WALLET-DEMO-EV";
    private static final double TOPUP_AMOUNT = 0.25; // small on purpose: exhausted within 2-3 billing cycles (~5-15s)

    public static void main(String[] args) throws Exception {
        try {
            java.rmi.server.RMISocketFactory.setSocketFactory(new java.rmi.server.RMISocketFactory() {
                @Override
                public java.net.Socket createSocket(String host, int port) throws java.io.IOException {
                    if (host != null && (host.startsWith("charging-station") || host.startsWith("reservation")
                            || host.startsWith("charging-session") || host.startsWith("pricing")
                            || host.startsWith("payment") || "time-server".equals(host)
                            || "manager".equals(host) || "ev-manager".equals(host))) {
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
        System.out.println("   WALLET DEMO -- CROSS-SERVER LAMPORT CLOCK DEMONSTRATION");
        System.out.println("============================================================");
        System.out.println("User: " + USER_ID + " | Top-up: Rs. " + TOPUP_AMOUNT);
        System.out.println();

        PaymentInterface payment = (PaymentInterface) Naming.lookup(managerUrl("PaymentService"));
        ReservationInterface reservation = (ReservationInterface) Naming.lookup(managerUrl("ReservationService"));
        ChargingSessionInterface chargingSession = (ChargingSessionInterface) Naming.lookup(managerUrl("ChargingSessionService"));

        // 1. Wallet top-up
        long sendL1 = clock.sendEvent();
        printCall("SEND", "addFunds(" + USER_ID + ", Rs. " + TOPUP_AMOUNT + ")", sendL1);
        LamportResult<String> fundsRes = payment.addFunds(USER_ID, TOPUP_AMOUNT, sendL1);
        clock.receiveEvent(fundsRes.getTimestamp());
        printCall("RECEIVE", "addFunds response: " + fundsRes.getData(), fundsRes.getTimestamp());

        // 2. Reserve a slot, then start charging
        long sendL2 = clock.sendEvent();
        printCall("SEND", "reserveSlot(" + USER_ID + ", " + VEHICLE_ID + ")", sendL2);
        LamportResult<String> reserveRes = reservation.reserveSlot(USER_ID, VEHICLE_ID, sendL2);
        clock.receiveEvent(reserveRes.getTimestamp());
        String reservationId = extractField(reserveRes.getData(), "Reservation ID:");
        printCall("RECEIVE", "reserveSlot response, Reservation ID=" + reservationId, reserveRes.getTimestamp());

        if (reservationId == null) {
            System.out.println("Could not reserve a slot (station may be full). Aborting demo.");
            return;
        }

        long sendL3 = clock.sendEvent();
        printCall("SEND", "startCharging(" + reservationId + ")", sendL3);
        LamportResult<String> startRes = chargingSession.startCharging(reservationId, sendL3);
        clock.receiveEvent(startRes.getTimestamp());
        String sessionId = extractField(startRes.getData(), "Session ID:");
        printCall("RECEIVE", "startCharging response, Session ID=" + sessionId, startRes.getTimestamp());

        if (sessionId == null) {
            System.out.println("Could not start charging session. Aborting demo.");
            return;
        }

        // 3. Poll session status while the ChargingSessionServer's background
        //    billing cycle repeatedly calls PaymentServer.checkAndDeductBalance
        //    (that RMI round trip is the actual cross-server Lamport chain --
        //    watch it live via `docker compose logs charging-session-1 payment-1 -f`)
        System.out.println();
        System.out.println("Polling session status every 3s. Watch the server logs for the");
        System.out.println("SEND(ChargingSession) -> RECEIVE(Payment) -> SEND(Payment) -> RECEIVE(ChargingSession)");
        System.out.println("billing cycle Lamport sequence, repeating every 5s, until the balance runs out.");
        System.out.println();

        String status = "CHARGING";
        int maxPolls = 20; // up to ~1 minute -- balance should exhaust within a couple billing cycles
        for (int i = 0; i < maxPolls && status.equals("CHARGING"); i++) {
            Thread.sleep(3000);
            long sendLp = clock.sendEvent();
            LamportResult<String> statusRes = chargingSession.getSessionStatus(sessionId, sendLp);
            clock.receiveEvent(statusRes.getTimestamp());
            status = extractField(statusRes.getData(), "Status:");
            printCall("POLL", "Session " + sessionId + " status=" + status, statusRes.getTimestamp());
        }

        System.out.println();
        if ("CHARGING".equals(status)) {
            System.out.println("Session still charging after " + maxPolls + " polls -- balance may not have run out yet. Stopping manually.");
            chargingSession.stopCharging(sessionId, clock.sendEvent());
        } else {
            System.out.println("Session auto-stopped by the wallet billing cycle (balance exhausted).");
        }

        // 4. Final wallet balance
        long sendL4 = clock.sendEvent();
        printCall("SEND", "getWalletBalance(" + USER_ID + ")", sendL4);
        LamportResult<Double> balRes = payment.getWalletBalance(USER_ID, sendL4);
        clock.receiveEvent(balRes.getTimestamp());
        printCall("RECEIVE", "getWalletBalance response: Rs. " + balRes.getData(), balRes.getTimestamp());

        System.out.println();
        System.out.println("============================================================");
        System.out.println("   DEMO COMPLETE");
        System.out.println("============================================================");
    }

    private static void printCall(String eventType, String message, long lamport) {
        System.out.println("[Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted()
                + "] [Lamport=" + lamport + "] [Client=WalletDemo] [Event=" + eventType + "] " + message);
    }

    private static String extractField(String text, String label) {
        if (text == null) return null;
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

    private static String managerUrl(String boundName) {
        String host = System.getenv("MANAGER_HOST");
        if (host == null || host.trim().isEmpty()) host = "localhost";
        String portStr = System.getenv("MANAGER_PORT");
        int port = 1240;
        if (portStr != null && !portStr.trim().isEmpty()) {
            try { port = Integer.parseInt(portStr.trim()); } catch (NumberFormatException ignored) {}
        }
        return "rmi://" + host + ":" + port + "/" + boundName;
    }
}
