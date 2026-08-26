import java.rmi.Naming;
import java.util.Scanner;
import Clock.LogicalClock;
import Clock.PhysicalClock;
import Clock.LamportResult;

// Unified EV / mobile-app style console client. This is an RMI client only --
// it never binds or exports anything itself, it just looks up the five
// existing RMI services and drives the same distributed flow the individual
// per-module clients exercise, from one menu.
public class EVClient {

    private static final String STATION_ID = "S01";

    private static String userId;
    private static String vehicleId;

    // Client-side Lamport Logical Clock
    private static final LogicalClock clientClock = new LogicalClock();

    // Remembers the most recent IDs so later menu options can default to them
    private static String lastReservationId = null;
    private static String lastSessionId = null;
    private static String lastPaymentId = null;

    public static void main(String[] args) {

        try {
            java.rmi.server.RMISocketFactory.setSocketFactory(new java.rmi.server.RMISocketFactory() {
                @Override
                public java.net.Socket createSocket(String host, int port) throws java.io.IOException {
                    if ("charging-station".equals(host) || "reservation".equals(host) ||
                        "charging-session".equals(host) || "pricing".equals(host) ||
                        "payment".equals(host) || "time-server".equals(host)) {
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

        Scanner sc = new Scanner(System.in);

        System.out.println("========================================");
        System.out.println("             EV CHARGING APP");
        System.out.println("========================================");

        System.out.print("Enter User ID: ");
        userId = sc.nextLine().trim();

        System.out.print("Enter Vehicle ID: ");
        vehicleId = sc.nextLine().trim();

        while (true) {
            printMenu();
            System.out.print("Enter your choice: ");

            String choiceLine = sc.nextLine().trim();
            int choice;

            try {
                choice = Integer.parseInt(choiceLine);
            } catch (NumberFormatException e) {
                System.out.println("Invalid choice.");
                continue;
            }

            switch (choice) {
                case 1: viewStationStatus(); break;
                case 2: viewAvailablePorts(); break;
                case 3: checkPortAvailability(sc); break;
                case 4: reserveSlot(); break;
                case 5: checkReservation(sc); break;
                case 6: cancelReservation(sc); break;
                case 7: startCharging(sc); break;
                case 8: checkSession(sc); break;
                case 9: stopCharging(sc); break;
                case 10: calculateBill(sc); break;
                case 11: makePayment(sc); break;
                case 12: checkPaymentStatus(sc); break;
                case 13: viewPaymentDetails(sc); break;
                case 14:
                    System.out.println("Exiting EV Charging App...");
                    sc.close();
                    return;
                default:
                    System.out.println("Invalid choice.");
            }
        }
    }

    private static void printMenu() {
        System.out.println();
        System.out.println("========================================");
        System.out.println("             EV CHARGING APP");
        System.out.println(" [Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + " | Lamport=" + clientClock.getValue() + "]");
        System.out.println("========================================");
        System.out.println("1. View Station Status");
        System.out.println("2. View Available Ports");
        System.out.println("3. Check Port Availability");
        System.out.println("4. Reserve Charging Slot");
        System.out.println("5. Check Reservation");
        System.out.println("6. Cancel Reservation");
        System.out.println("7. Start Charging");
        System.out.println("8. Check Charging Session");
        System.out.println("9. Stop Charging");
        System.out.println("10. Calculate Bill");
        System.out.println("11. Make Payment");
        System.out.println("12. Check Payment Status");
        System.out.println("13. View Payment Details");
        System.out.println("14. Exit");
        System.out.println("========================================");
    }

    private static String getEnvHost(String envVar, String defaultHost) {
        String host = System.getenv(envVar);
        return (host != null && !host.trim().isEmpty()) ? host.trim() : defaultHost;
    }

    private static ChargingStationInterface lookupChargingStation() throws Exception {
        return (ChargingStationInterface) Naming.lookup(
                "rmi://" + getEnvHost("STATION_HOST", "localhost") + ":1234//ChargingStationServer"
        );
    }

    private static ReservationInterface lookupReservation() throws Exception {
        return (ReservationInterface) Naming.lookup(
                "rmi://" + getEnvHost("RESERVATION_HOST", "localhost") + ":1235/ReservationService"
        );
    }

    private static ChargingSessionInterface lookupChargingSession() throws Exception {
        return (ChargingSessionInterface) Naming.lookup(
                "rmi://" + getEnvHost("SESSION_HOST", "localhost") + ":1236/ChargingSessionServer"
        );
    }

    private static PaymentInterface lookupPayment() throws Exception {
        return (PaymentInterface) Naming.lookup(
                "rmi://" + getEnvHost("PAYMENT_HOST", "localhost") + ":1237/PaymentServer"
        );
    }

    private static PricingInterface lookupPricing() throws Exception {
        return (PricingInterface) Naming.lookup(
                "rmi://" + getEnvHost("PRICING_HOST", "localhost") + ":1238/PricingService"
        );
    }

    // ---------------------------------------------------------------
    // Menu operations
    // ---------------------------------------------------------------

    private static void viewStationStatus() {
        try {
            ChargingStationInterface chargingStation = lookupChargingStation();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = chargingStation.getStationStatus(sendL);
            clientClock.receiveEvent(res.getTimestamp());

            System.out.println();
            System.out.println(res.getData());
        } catch (Exception e) {
            stationUnavailable();
        }
    }

    private static void viewAvailablePorts() {
        try {
            ChargingStationInterface chargingStation = lookupChargingStation();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = chargingStation.getAvailablePorts(sendL);
            clientClock.receiveEvent(res.getTimestamp());

            System.out.println();
            System.out.println(res.getData());
        } catch (Exception e) {
            stationUnavailable();
        }
    }

    private static void checkPortAvailability(Scanner sc) {
        System.out.print("Enter Port ID (P1/P2/P3/P4): ");
        String portId = sc.nextLine().trim();

        try {
            ChargingStationInterface chargingStation = lookupChargingStation();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = chargingStation.checkPortAvailability(portId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            System.out.println();
            System.out.println(res.getData());
        } catch (Exception e) {
            stationUnavailable();
        }
    }

    private static void reserveSlot() {
        try {
            ReservationInterface reservation = lookupReservation();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = reservation.reserveSlot(userId, vehicleId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            String result = res.getData();
            System.out.println();
            System.out.println(result);

            String reservationId = extractField(result, "Reservation ID:");
            if (reservationId != null) {
                lastReservationId = reservationId;
            }
        } catch (Exception e) {
            reservationUnavailable();
        }
    }

    private static void checkReservation(Scanner sc) {
        String reservationId = promptWithDefault(sc, "Enter Reservation ID", lastReservationId);

        try {
            ReservationInterface reservation = lookupReservation();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = reservation.getReservation(reservationId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            System.out.println();
            System.out.println(res.getData());
        } catch (Exception e) {
            reservationUnavailable();
        }
    }

    private static void cancelReservation(Scanner sc) {
        String reservationId = promptWithDefault(sc, "Enter Reservation ID", lastReservationId);

        try {
            ReservationInterface reservation = lookupReservation();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = reservation.cancelReservation(reservationId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            String result = res.getData();
            System.out.println();
            System.out.println(result);

            if (reservationId.equals(lastReservationId)) {
                lastReservationId = null;
            }
        } catch (Exception e) {
            reservationUnavailable();
        }
    }

    private static void startCharging(Scanner sc) {
        String reservationId = promptWithDefault(sc, "Enter Reservation ID", lastReservationId);

        try {
            ChargingSessionInterface chargingSession = lookupChargingSession();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = chargingSession.startCharging(reservationId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            String result = res.getData();
            System.out.println();
            System.out.println(result);

            String sessionId = extractField(result, "Session ID:");
            if (sessionId != null) {
                lastSessionId = sessionId;
            }
        } catch (Exception e) {
            sessionUnavailable();
        }
    }

    private static void checkSession(Scanner sc) {
        String sessionId = promptWithDefault(sc, "Enter Session ID", lastSessionId);

        try {
            ChargingSessionInterface chargingSession = lookupChargingSession();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = chargingSession.getSessionStatus(sessionId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            System.out.println();
            System.out.println(res.getData());
        } catch (Exception e) {
            sessionUnavailable();
        }
    }

    private static void stopCharging(Scanner sc) {
        String sessionId = promptWithDefault(sc, "Enter Session ID", lastSessionId);

        try {
            ChargingSessionInterface chargingSession = lookupChargingSession();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = chargingSession.stopCharging(sessionId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            System.out.println();
            System.out.println(res.getData());
        } catch (Exception e) {
            sessionUnavailable();
        }
    }

    private static void calculateBill(Scanner sc) {
        String sessionId = promptWithDefault(sc, "Enter Session ID", lastSessionId);

        double energy;
        try {
            ChargingSessionInterface chargingSession = lookupChargingSession();
            long sendL = clientClock.sendEvent();
            LamportResult<Double> energyRes = chargingSession.getEnergyConsumed(sessionId, sendL);
            clientClock.receiveEvent(energyRes.getTimestamp());
            energy = energyRes.getData();
        } catch (Exception e) {
            sessionUnavailable();
            return;
        }

        if (energy < 0) {
            System.out.println();
            System.out.println("No energy data found for session " + sessionId + " (has charging finished yet?).");
            return;
        }

        try {
            PricingInterface pricing = lookupPricing();
            long sendL = clientClock.sendEvent();
            LamportResult<Double> priceRes = pricing.calculatePrice(STATION_ID, energy, sendL);
            clientClock.receiveEvent(priceRes.getTimestamp());
            double price = priceRes.getData();

            System.out.println();
            System.out.println("Session ID: " + sessionId);
            System.out.println("Energy Consumed: " + energy + " kWh");

            if (price < 0) {
                System.out.println("Pricing Server returned an invalid price.");
            } else {
                System.out.println("Estimated Bill: Rs. " + String.format("%.2f", price));
            }
        } catch (Exception e) {
            pricingUnavailable();
        }
    }

    private static void makePayment(Scanner sc) {
        String sessionId = promptWithDefault(sc, "Enter Session ID", lastSessionId);

        try {
            PaymentInterface payment = lookupPayment();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = payment.makePayment(sessionId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            String result = res.getData();
            System.out.println();
            System.out.println(result);

            String paymentId = extractField(result, "Payment ID:");
            if (paymentId != null) {
                lastPaymentId = paymentId;
            }
        } catch (Exception e) {
            paymentUnavailable();
        }
    }

    private static void checkPaymentStatus(Scanner sc) {
        String paymentId = promptWithDefault(sc, "Enter Payment ID", lastPaymentId);

        try {
            PaymentInterface payment = lookupPayment();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = payment.getPaymentStatus(paymentId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            System.out.println();
            System.out.println(res.getData());
        } catch (Exception e) {
            paymentUnavailable();
        }
    }

    private static void viewPaymentDetails(Scanner sc) {
        String paymentId = promptWithDefault(sc, "Enter Payment ID", lastPaymentId);

        try {
            PaymentInterface payment = lookupPayment();
            long sendL = clientClock.sendEvent();
            LamportResult<String> res = payment.getPaymentDetails(paymentId, sendL);
            clientClock.receiveEvent(res.getTimestamp());

            System.out.println();
            System.out.println(res.getData());
        } catch (Exception e) {
            paymentUnavailable();
        }
    }

    private static String promptWithDefault(Scanner sc, String label, String defaultValue) {
        if (defaultValue != null) {
            System.out.print(label + " [" + defaultValue + "]: ");
        } else {
            System.out.print(label + ": ");
        }

        String input = sc.nextLine().trim();

        if (input.isEmpty() && defaultValue != null) {
            return defaultValue;
        }

        return input;
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

    private static void stationUnavailable() {
        System.out.println("\nCharging Station Server unavailable.\nPlease start the ChargingStationServer first.");
    }
    private static void reservationUnavailable() {
        System.out.println("\nReservation Server unavailable.\nPlease start the ReservationServer first.");
    }
    private static void sessionUnavailable() {
        System.out.println("\nCharging Session Server unavailable.\nPlease start the ChargingSessionServer first.");
    }
    private static void pricingUnavailable() {
        System.out.println("\nPricing Server unavailable.\nPlease start the PricingServer first.");
    }
    private static void paymentUnavailable() {
        System.out.println("\nPayment Server unavailable.\nPlease start the PaymentServer first.");
    }
}
