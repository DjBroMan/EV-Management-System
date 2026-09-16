import java.sql.*;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Data Access Object for the charging_sessions table (ev_session_db).
 *
 * Operations:
 *   startCharging -> insertSession  (status = CHARGING)
 *   stopCharging  -> completeSession (status = COMPLETED, end_time, energy filled in)
 *   startup       -> loadAllSessions + getMaxCounter
 */
public class ChargingSessionDAO {

    private static final String SERVER_NAME = "ChargingSessionDAO";

    // ----------------------------------------------------------------
    // Startup: recover sessions and counter
    // ----------------------------------------------------------------

    /**
     * Returns the numeric suffix of the highest session_id, or 1000 if empty.
     * session_id format: "SESSION-1001" -> SUBSTRING from position 9 = "1001"
     */
    public int getMaxCounter() throws Exception {
        String sql = "SELECT COALESCE("
                   + "    MAX(CAST(SUBSTRING(session_id, 9) AS UNSIGNED)), "
                   + "    1000) "
                   + "FROM charging_sessions";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            rs.next();
            int maxCounter = rs.getInt(1);
            System.out.println("[DB:" + SERVER_NAME + "] Max session counter from DB: "
                    + maxCounter);
            return maxCounter;
        }
    }

    /**
     * Loads all session records from DB into the provided in-memory maps.
     * Restores session state so the server can resume after a restart.
     *
     * @param reservationSessions  reservation_id -> session_id
     * @param sessionStatus        session_id -> status
     * @param energyConsumed       session_id -> energy in kWh
     * @param sessionPort          session_id -> port_id
     * @param sessionStartTimes    session_id -> Instant start
     * @param sessionEndTimes      session_id -> Instant end (null if still CHARGING)
     * @param sessionChargingPowers session_id -> charging power in kW
     */
    public void loadAllSessions(
            Map<String, String>  reservationSessions,
            Map<String, String>  sessionStatus,
            Map<String, Double>  energyConsumed,
            Map<String, String>  sessionPort,
            Map<String, Instant> sessionStartTimes,
            Map<String, Instant> sessionEndTimes,
            Map<String, Double>  sessionChargingPowers) throws Exception {

        String sql = "SELECT session_id, reservation_id, port_id, status, "
                   + "       start_time, end_time, charging_power_kw, energy_consumed_kwh "
                   + "FROM charging_sessions";

        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {

            int count = 0;
            while (rs.next()) {
                String sessionId     = rs.getString("session_id");
                String reservationId = rs.getString("reservation_id");

                reservationSessions.put(reservationId, sessionId);
                sessionStatus.put(sessionId,      rs.getString("status"));
                sessionPort.put(sessionId,         rs.getString("port_id"));
                sessionChargingPowers.put(sessionId, rs.getDouble("charging_power_kw"));
                energyConsumed.put(sessionId,      rs.getDouble("energy_consumed_kwh"));

                Timestamp startTs = rs.getTimestamp("start_time");
                if (startTs != null) {
                    sessionStartTimes.put(sessionId, startTs.toInstant());
                }
                Timestamp endTs = rs.getTimestamp("end_time");
                if (endTs != null) {
                    sessionEndTimes.put(sessionId, endTs.toInstant());
                }
                count++;
            }
            System.out.println("[DB:" + SERVER_NAME + "] Loaded " + count
                    + " session records.");
        }
    }

    // ----------------------------------------------------------------
    // Runtime: write operations
    // ----------------------------------------------------------------

    /**
     * Inserts a new charging session row with status CHARGING.
     * Called by ChargingSessionServer.startCharging after the in-memory maps
     * are updated inside the synchronized block.
     *
     * @param startTimeMs PhysicalClock.getSynchronizedPhysicalTimeMillis()
     */
    public void insertSession(String sessionId, String reservationId, String portId,
                               double chargingPowerKw, long startTimeMs) throws Exception {
        String sql = "INSERT INTO charging_sessions "
                   + "(session_id, reservation_id, port_id, status, "
                   + " start_time, charging_power_kw, energy_consumed_kwh) "
                   + "VALUES (?, ?, ?, 'CHARGING', ?, ?, 0.0000)";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, sessionId);
            ps.setString(2, reservationId);
            ps.setString(3, portId);
            ps.setTimestamp(4, new Timestamp(startTimeMs));
            ps.setDouble(5, chargingPowerKw);
            ps.executeUpdate();
        }
    }

    /**
     * Marks a session as COMPLETED, recording end time and final energy.
     * Called by ChargingSessionServer.stopCharging after the in-memory maps
     * are updated inside the synchronized block.
     *
     * @param endTimeMs   PhysicalClock.getSynchronizedPhysicalTimeMillis()
     * @param energyKwh   calculated energy consumed (kWh)
     */
    public void completeSession(String sessionId, long endTimeMs, double energyKwh)
            throws Exception {
        String sql = "UPDATE charging_sessions "
                   + "SET status = 'COMPLETED', end_time = ?, energy_consumed_kwh = ? "
                   + "WHERE session_id = ?";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setTimestamp(1, new Timestamp(endTimeMs));
            ps.setDouble(2, energyKwh);
            ps.setString(3, sessionId);
            ps.executeUpdate();
        }
    }
}
