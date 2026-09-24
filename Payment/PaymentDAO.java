import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Data Access Object for the payments table (ev_payment_db).
 *
 * Operations:
 *   makePayment       -> insertPayment
 *   getPaymentStatus  -> reads from in-memory map (DB fallback if not found)
 *   getPaymentDetails -> reads from in-memory map (DB fallback if not found)
 *   startup           -> loadAllPayments + getMaxCounter
 */
public class PaymentDAO {

    private static final String SERVER_NAME = "PaymentDAO";

    // ----------------------------------------------------------------
    // Startup: recover payments and counter
    // ----------------------------------------------------------------

    /**
     * Returns the numeric suffix of the highest payment_id, or 1000 if empty.
     * payment_id format: "PAY-1001" -> SUBSTRING from position 5 = "1001"
     */
    public int getMaxCounter() throws Exception {
        String sql = "SELECT COALESCE("
                   + "    MAX(CAST(SUBSTRING(payment_id, 5) AS UNSIGNED)), "
                   + "    1000) "
                   + "FROM payments";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            rs.next();
            int maxCounter = rs.getInt(1);
            System.out.println("[DB:" + SERVER_NAME + "] Max payment counter from DB: "
                    + maxCounter);
            return maxCounter;
        }
    }

    /**
     * Loads all payment records into the provided in-memory maps.
     * Reconstructs the string format used by PaymentServer.paymentDetails.
     *
     * @param paymentStatus  paymentId -> "SUCCESS"
     * @param paymentDetails paymentId -> formatted detail string
     */
    public void loadAllPayments(Map<String, String> paymentStatus,
                                 Map<String, String> paymentDetails) throws Exception {
        String sql = "SELECT payment_id, session_id, station_id, "
                   + "       energy_consumed_kwh, total_amount, payment_status "
                   + "FROM payments";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                String payId  = rs.getString("payment_id");
                String status = rs.getString("payment_status");
                double energy = rs.getDouble("energy_consumed_kwh");
                double amount = rs.getDouble("total_amount");

                paymentStatus.put(payId, status);

                // Reconstruct details string in the same format PaymentServer uses
                String details = "Payment ID: "       + payId
                               + "\nSession ID: "      + rs.getString("session_id")
                               + "\nEnergy Consumed: " + energy + " kWh"
                               + "\nAmount: Rs. "      + amount
                               + "\nPayment Status: "  + status;
                paymentDetails.put(payId, details);
                count++;
            }
            System.out.println("[DB:" + SERVER_NAME + "] Loaded " + count
                    + " payment records.");
        }
    }

    // ----------------------------------------------------------------
    // Runtime: write operations
    // ----------------------------------------------------------------

    /**
     * Inserts a new payment record with status SUCCESS.
     * Called by PaymentServer.makePayment after the in-memory maps are updated.
     *
     * @param timestampMs PhysicalClock.getSynchronizedPhysicalTimeMillis()
     */
    public void insertPayment(String paymentId, String sessionId, String stationId,
                               double energyKwh, double totalAmount, long timestampMs)
            throws Exception {
        String sql = "INSERT INTO payments "
                   + "(payment_id, session_id, station_id, energy_consumed_kwh, "
                   + " total_amount, payment_status, payment_time) "
                   + "VALUES (?, ?, ?, ?, ?, 'SUCCESS', ?)";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, paymentId);
            ps.setString(2, sessionId);
            ps.setString(3, stationId);
            ps.setDouble(4, energyKwh);
            ps.setDouble(5, totalAmount);
            ps.setTimestamp(6, new Timestamp(timestampMs));
            ps.executeUpdate();
        }
    }

    // ----------------------------------------------------------------
    // Fallback queries (used when in-memory map does not have the record)
    // ----------------------------------------------------------------

    /**
     * Queries payment_status for a single payment ID.
     * Returns null if not found.
     */
    public String queryPaymentStatus(String paymentId) throws Exception {
        String sql = "SELECT payment_status FROM payments WHERE payment_id = ?";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, paymentId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("payment_status") : null;
            }
        }
    }

    /**
     * Queries full payment details for a single payment ID.
     * Returns null if not found.
     */
    public String queryPaymentDetails(String paymentId) throws Exception {
        String sql = "SELECT payment_id, session_id, energy_consumed_kwh, "
                   + "       total_amount, payment_status "
                   + "FROM payments WHERE payment_id = ?";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, paymentId);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return null;
                return "Payment ID: "       + rs.getString("payment_id")
                     + "\nSession ID: "      + rs.getString("session_id")
                     + "\nEnergy Consumed: " + rs.getDouble("energy_consumed_kwh") + " kWh"
                     + "\nAmount: Rs. "      + rs.getDouble("total_amount")
                     + "\nPayment Status: "  + rs.getString("payment_status");
            }
        }
    }

    // ----------------------------------------------------------------
    // Wallet operations (ev_payment_db.wallets)
    // ----------------------------------------------------------------

    /**
     * Loads all wallet balances into the provided in-memory map.
     */
    public void loadAllWallets(Map<String, Double> walletBalances) throws Exception {
        String sql = "SELECT user_id, balance FROM wallets";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            int count = 0;
            while (rs.next()) {
                walletBalances.put(rs.getString("user_id"), rs.getDouble("balance"));
                count++;
            }
            System.out.println("[DB:" + SERVER_NAME + "] Loaded " + count
                    + " wallet records.");
        }
    }

    /**
     * Inserts or updates a user's wallet balance.
     *
     * @param timestampMs PhysicalClock.getSynchronizedPhysicalTimeMillis()
     */
    public void upsertWalletBalance(String userId, double newBalance, long timestampMs)
            throws Exception {
        String sql = "INSERT INTO wallets (user_id, balance, last_updated) "
                   + "VALUES (?, ?, ?) "
                   + "ON DUPLICATE KEY UPDATE balance = VALUES(balance), "
                   + "last_updated = VALUES(last_updated)";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            ps.setDouble(2, newBalance);
            ps.setTimestamp(3, new Timestamp(timestampMs));
            ps.executeUpdate();
        }
    }

    /**
     * Queries the wallet balance for a single user. Returns null if not found.
     */
    public Double queryWalletBalance(String userId) throws Exception {
        String sql = "SELECT balance FROM wallets WHERE user_id = ?";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, userId);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getDouble("balance") : null;
            }
        }
    }
}
