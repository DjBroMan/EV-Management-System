import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Data Access Object for the reservations table.
 *
 * Used by both ReservationServer PRIMARY (ev_reservation_primary_db) and
 * ReservationServer SECONDARY (ev_reservation_secondary_db). The target
 * database is determined entirely by the DB_NAME environment variable —
 * no code changes are needed between the two instances.
 *
 * Operations:
 *   reserveSlot            -> insertReservation
 *   cancelReservation      -> deleteReservation  (hard DELETE, matches HashMap.remove)
 *   applyReservationUpdate -> insertReservation  (secondary replication write)
 *   applyCancellationUpdate -> deleteReservation (secondary replication write)
 *   synchronizeFullState   -> deleteAllReservations + insertReservation * N
 *   resetState             -> deleteAllReservations
 *   startup                -> loadAllReservations + loadAllReservationPorts + getMaxCounter
 */
public class ReservationDAO {

    private static final String SERVER_NAME = "ReservationDAO";

    // ----------------------------------------------------------------
    // Startup: recover state and counter
    // ----------------------------------------------------------------

    /**
     * Returns the numeric suffix of the highest reservation_id stored in the DB,
     * or 1000 if the table is empty. The server sets its counter to maxCounter + 1.
     *
     * reservation_id format: "RES1001" -> SUBSTRING from position 4 = "1001"
     */
    public int getMaxCounter() throws Exception {
        String sql = "SELECT COALESCE("
                   + "    MAX(CAST(SUBSTRING(reservation_id, 4) AS UNSIGNED)), "
                   + "    1000) "
                   + "FROM reservations";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            rs.next();
            int maxCounter = rs.getInt(1);
            System.out.println("[DB:" + SERVER_NAME + "] Max reservation counter from DB: "
                    + maxCounter);
            return maxCounter;
        }
    }

    /**
     * Loads all reservation detail strings into the in-memory reservations map.
     * Format mirrors the existing HashMap value format used by ReservationServer:
     *   "Reservation ID: RES1001, User ID: u1, Vehicle ID: EV-1, Port: P1, Status: CONFIRMED"
     */
    public Map<String, String> loadAllReservations() throws Exception {
        Map<String, String> result = new HashMap<>();
        String sql = "SELECT reservation_id, user_id, vehicle_id, port_id, status "
                   + "FROM reservations";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            while (rs.next()) {
                String resId   = rs.getString("reservation_id");
                String userId  = rs.getString("user_id");
                String vehId   = rs.getString("vehicle_id");
                String portId  = rs.getString("port_id");
                String status  = rs.getString("status");
                // Reconstruct the same string format used by in-memory ReservationServer
                String details = "Reservation ID: " + resId
                               + ", User ID: "     + userId
                               + ", Vehicle ID: "  + vehId
                               + ", Port: "        + portId
                               + ", Status: "      + status;
                result.put(resId, details);
            }
        }
        System.out.println("[DB:" + SERVER_NAME + "] Loaded " + result.size()
                + " reservation records.");
        return result;
    }

    /**
     * Loads reservation_id -> port_id mapping from the DB into the
     * in-memory reservationPorts map.
     */
    public Map<String, String> loadAllReservationPorts() throws Exception {
        Map<String, String> result = new HashMap<>();
        String sql = "SELECT reservation_id, port_id FROM reservations";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            while (rs.next()) {
                result.put(rs.getString("reservation_id"), rs.getString("port_id"));
            }
        }
        return result;
    }

    // ----------------------------------------------------------------
    // Runtime: write operations
    // ----------------------------------------------------------------

    /**
     * Inserts a new confirmed reservation.
     * Called by PRIMARY on reserveSlot and by SECONDARY on applyReservationUpdate.
     *
     * @param timestampMs PhysicalClock.getSynchronizedPhysicalTimeMillis()
     */
    public void insertReservation(String reservationId, String userId, String vehicleId,
                                   String portId, long timestampMs) throws Exception {
        String sql = "INSERT INTO reservations "
                   + "(reservation_id, user_id, vehicle_id, port_id, status, created_at) "
                   + "VALUES (?, ?, ?, ?, 'CONFIRMED', ?)";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, reservationId);
            ps.setString(2, userId);
            ps.setString(3, vehicleId);
            ps.setString(4, portId);
            ps.setTimestamp(5, new Timestamp(timestampMs));
            ps.executeUpdate();
        }
    }

    /**
     * Deletes a reservation row (hard delete, matching HashMap.remove behavior).
     * Called by PRIMARY and SECONDARY on cancel/applyCancellationUpdate.
     */
    public void deleteReservation(String reservationId) throws Exception {
        String sql = "DELETE FROM reservations WHERE reservation_id = ?";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, reservationId);
            ps.executeUpdate();
        }
    }

    /**
     * Deletes ALL rows from the reservations table.
     * Called during synchronizeFullState (before re-inserting snapshot) and resetState.
     */
    public void deleteAllReservations() throws Exception {
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement()) {
            int deleted = st.executeUpdate("DELETE FROM reservations");
            System.out.println("[DB:" + SERVER_NAME + "] Deleted " + deleted
                    + " reservation records.");
        }
    }
}
