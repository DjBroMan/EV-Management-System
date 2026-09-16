package Common;

/**
 * Shared URL-resolution helper for server-to-server (not client-to-Manager)
 * RMI calls. Every cluster now has N instances behind a Bully-elected
 * leader, so a fixed direct port no longer reliably reaches a working
 * instance; the default for all inter-service calls is therefore to route
 * through the Manager's load-balanced/leader-aware proxy, exactly like
 * EVClient does. The *_HOST / *_URL environment variables remain available
 * as explicit overrides for direct single-instance manual testing.
 */
public class ManagerRouting {

    private static String getEnv(String key) {
        String v = System.getenv(key);
        return (v != null && !v.trim().isEmpty()) ? v.trim() : null;
    }

    private static String managerBase() {
        String host = getEnv("MANAGER_HOST");
        if (host == null) host = "localhost";
        String portStr = getEnv("MANAGER_PORT");
        int port = 1240;
        if (portStr != null) {
            try { port = Integer.parseInt(portStr); } catch (NumberFormatException ignored) { }
        }
        return "rmi://" + host + ":" + port + "/";
    }

    public static String resolveChargingStationUrl() {
        String explicit = getEnv("STATION_URL");
        if (explicit != null) return explicit;
        String host = getEnv("STATION_HOST");
        if (host != null) return "rmi://" + host + ":1234//ChargingStationServer";
        return managerBase() + "ChargingStationService";
    }

    public static String resolveChargingSessionUrl() {
        String explicit = getEnv("SESSION_URL");
        if (explicit != null) return explicit;
        String host = getEnv("SESSION_HOST");
        if (host != null) return "rmi://" + host + ":1236/ChargingSessionServer";
        return managerBase() + "ChargingSessionService";
    }

    public static String resolvePricingUrl() {
        String explicit = getEnv("PRICING_URL");
        if (explicit != null) return explicit;
        String host = getEnv("PRICING_HOST");
        if (host != null) return "rmi://" + host + ":1238/PricingService";
        return managerBase() + "PricingService";
    }

    public static String resolveReservationUrl() {
        String explicit = getEnv("RESERVATION_URL");
        if (explicit != null) return explicit;
        String host = getEnv("MANAGER_HOST");
        if (host == null) host = getEnv("RESERVATION_HOST");
        if (host == null) host = "localhost";
        String portStr = getEnv("MANAGER_PORT");
        if (portStr == null) portStr = getEnv("RESERVATION_PORT");
        int port = 1240;
        if (portStr != null) {
            try { port = Integer.parseInt(portStr); } catch (NumberFormatException ignored) { }
        }
        return "rmi://" + host + ":" + port + "/ReservationService";
    }
}
