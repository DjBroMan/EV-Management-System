import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Shared JDBC connection factory for all EV Charging Network servers.
 *
 * Reads connection parameters exclusively from environment variables:
 *   DB_HOST     - MySQL hostname         (default: localhost)
 *   DB_PORT     - MySQL port             (default: 3306)
 *   DB_NAME     - Database/schema name   (default: ev_db)
 *   DB_USER     - MySQL username         (default: root)
 *   DB_PASSWORD - MySQL password         (default: evroot)
 *
 * This class is in the default package so that all server classes
 * (ChargingStationServer, ReservationServer, etc.) and their DAO
 * classes can use it without any import statement.
 */
public class DBConnectionHelper {

    /** Returns true when DB_HOST is set, indicating DB mode is intended. */
    public static boolean isDatabaseConfigured() {
        String host = System.getenv("DB_HOST");
        return host != null && !host.trim().isEmpty();
    }

    /**
     * Opens and returns a new JDBC connection using environment variable
     * configuration. The caller is responsible for closing the connection.
     *
     * @throws Exception if the driver class is not found or connection fails
     */
    public static Connection getConnection() throws Exception {
        String host = getEnv("DB_HOST", "localhost");
        String port = getEnv("DB_PORT", "3306");
        String dbName = getEnv("DB_NAME", "ev_db");
        String user = getEnv("DB_USER", "root");
        String password = getEnv("DB_PASSWORD", "evroot");

        String url = "jdbc:mysql://" + host + ":" + port + "/" + dbName
                + "?useSSL=false"
                + "&allowPublicKeyRetrieval=true"
                + "&serverTimezone=UTC"
                + "&connectTimeout=5000"
                + "&socketTimeout=30000";

        Class.forName("com.mysql.cj.jdbc.Driver");
        return DriverManager.getConnection(url, user, password);
    }

    /**
     * Attempts to open a connection, retrying up to maxRetries times
     * with a 2-second pause between attempts. Used during server startup
     * to wait for the MySQL container to become fully ready.
     *
     * @param serverName  identifier used in log messages
     * @param maxRetries  maximum number of connection attempts
     * @return an open Connection, or null if all attempts fail
     */
    public static Connection getConnectionWithRetry(String serverName, int maxRetries) {
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                Connection conn = getConnection();
                System.out.println("[DB:" + serverName + "] Connected to database (attempt " + attempt + ").");
                return conn;
            } catch (Exception e) {
                System.out.println("[DB:" + serverName + "] Connection attempt " + attempt
                        + "/" + maxRetries + " failed: " + e.getMessage());
                if (attempt < maxRetries) {
                    try { Thread.sleep(2000); } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        System.out.println("[DB:" + serverName + "] All connection attempts failed. Running without database persistence.");
        return null;
    }

    // ----------------------------------------------------------------

    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.trim().isEmpty()) ? value.trim() : defaultValue;
    }
}
