import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Data Access Object for the charging_ports table (ev_station_db).
 *
 * Responsibilities:
 *   - Self-seed P1–P4 on first startup when table is empty (Option A)
 *   - Load current port statuses from DB into the in-memory portStatus[]
 *   - Persist status changes (AVAILABLE / RESERVED / CHARGING) on each operation
 *
 * Each method opens and closes its own connection to avoid stale connections
 * in long-running RMI servers. DBConnectionHelper reads DB_* env vars.
 */
public class ChargingStationDAO {

    private static final String SERVER_NAME = "ChargingStationDAO";

    // ----------------------------------------------------------------
    // Startup: self-seed and load
    // ----------------------------------------------------------------

    /**
     * Returns true when the charging_ports table contains zero rows.
     */
    public boolean isTableEmpty() throws Exception {
        String sql = "SELECT COUNT(*) FROM charging_ports";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1) == 0;
        }
    }

    /**
     * Inserts P1–P4 (or any given set of ports) into the DB if the table is
     * empty. Called once during server startup (Option A self-seeding).
     *
     * @param ports      port IDs (e.g., {"P1","P2","P3","P4"})
     * @param portStatus initial statuses (e.g., {"AVAILABLE", ...})
     * @param stationId  the logical station these ports belong to ("EV-STATION-01")
     * @param timestampMs PhysicalClock.getSynchronizedPhysicalTimeMillis()
     */
    public void initPortsIfEmpty(String[] ports, String[] portStatus,
                                  String stationId, long timestampMs) throws Exception {
        if (!isTableEmpty()) {
            return; // already seeded on a previous startup
        }
        String sql = "INSERT INTO charging_ports (port_id, station_id, status, last_updated) "
                   + "VALUES (?, ?, ?, ?)";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            for (int i = 0; i < ports.length; i++) {
                ps.setString(1, ports[i]);
                ps.setString(2, stationId);
                ps.setString(3, portStatus[i]);
                ps.setTimestamp(4, new Timestamp(timestampMs));
                ps.addBatch();
            }
            ps.executeBatch();
            System.out.println("[DB:" + SERVER_NAME + "] Self-seeded " + ports.length
                    + " ports into charging_ports.");
        }
    }

    /**
     * Loads all rows from charging_ports and returns a map of portId -> status.
     * Called during startup to restore persisted port states into portStatus[].
     */
    public Map<String, String> loadAllPorts() throws Exception {
        Map<String, String> result = new LinkedHashMap<>();
        String sql = "SELECT port_id, status FROM charging_ports ORDER BY port_id";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            while (rs.next()) {
                result.put(rs.getString("port_id"), rs.getString("status"));
            }
        }
        System.out.println("[DB:" + SERVER_NAME + "] Loaded " + result.size()
                + " port records from DB.");
        return result;
    }

    // ----------------------------------------------------------------
    // Runtime: persist status changes
    // ----------------------------------------------------------------

    /**
     * Updates the status and last_updated timestamp for a single port.
     * Called after every in-memory portStatus[] mutation.
     *
     * @param portId      the port whose status changed
     * @param status      new status string (AVAILABLE / RESERVED / CHARGING)
     * @param timestampMs PhysicalClock.getSynchronizedPhysicalTimeMillis()
     */
    public void updatePortStatus(String portId, String status, long timestampMs)
            throws Exception {
        String sql = "UPDATE charging_ports "
                   + "SET status = ?, last_updated = ? "
                   + "WHERE port_id = ?";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, status);
            ps.setTimestamp(2, new Timestamp(timestampMs));
            ps.setString(3, portId);
            ps.executeUpdate();
        }
    }
}
