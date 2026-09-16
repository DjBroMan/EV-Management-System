import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Data Access Object for the station_pricing_tariffs table (ev_pricing_db).
 *
 * Responsibilities:
 *   - Self-seed S01/S02/S03 on first startup when table is empty (Option A)
 *   - Load demand levels into the in-memory stationDemand map on startup
 *
 * PricingServer does not update tariffs at runtime (no mutation operations
 * during normal EV workflow), so only startup operations are needed.
 */
public class PricingDAO {

    private static final String SERVER_NAME = "PricingDAO";

    // ----------------------------------------------------------------
    // Startup: self-seed and load
    // ----------------------------------------------------------------

    /**
     * Returns true when station_pricing_tariffs contains zero rows.
     */
    public boolean isTableEmpty() throws Exception {
        String sql = "SELECT COUNT(*) FROM station_pricing_tariffs";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            rs.next();
            return rs.getInt(1) == 0;
        }
    }

    /**
     * Inserts a single station tariff row.
     */
    public void insertTariff(String stationId, double basePricePerKwh,
                              String demandLevel, double demandMultiplier) throws Exception {
        String sql = "INSERT INTO station_pricing_tariffs "
                   + "(station_id, base_price_per_kwh, demand_level, demand_multiplier) "
                   + "VALUES (?, ?, ?, ?)";
        try (Connection conn = DBConnectionHelper.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, stationId);
            ps.setDouble(2, basePricePerKwh);
            ps.setString(3, demandLevel);
            ps.setDouble(4, demandMultiplier);
            ps.executeUpdate();
        }
    }

    /**
     * Self-seeds S01/S02/S03 from the PricingServer's hardcoded stationDemand
     * map if the table is empty. The BASE_PRICE is passed in from PricingServer
     * to avoid duplicating the constant here.
     *
     * Demand multipliers match PricingServer.calculatePrice() switch logic:
     *   LOW    -> 1.00
     *   MEDIUM -> 1.25
     *   HIGH   -> 1.50
     *
     * @param stationDemandMap the existing PricingServer.stationDemand map
     * @param basePrice        PricingServer.BASE_PRICE (currently 10.0)
     */
    public void initTariffsIfEmpty(Map<String, String> stationDemandMap,
                                    double basePrice) throws Exception {
        if (!isTableEmpty()) {
            return; // already seeded on a previous startup
        }

        Map<String, Double> multiplierMap = new HashMap<>();
        multiplierMap.put("LOW",    1.0);
        multiplierMap.put("MEDIUM", 1.25);
        multiplierMap.put("HIGH",   1.50);

        for (Map.Entry<String, String> entry : stationDemandMap.entrySet()) {
            String stationId   = entry.getKey();
            String demandLevel = entry.getValue();
            double multiplier  = multiplierMap.getOrDefault(demandLevel, 1.0);
            insertTariff(stationId, basePrice, demandLevel, multiplier);
        }
        System.out.println("[DB:" + SERVER_NAME + "] Self-seeded " + stationDemandMap.size()
                + " pricing tariff records into station_pricing_tariffs.");
    }

    /**
     * Loads all station demand levels from the DB into a map of stationId -> demandLevel.
     * Used to populate (or refresh) PricingServer.stationDemand on startup.
     */
    public Map<String, String> loadAllTariffs() throws Exception {
        Map<String, String> result = new HashMap<>();
        String sql = "SELECT station_id, demand_level FROM station_pricing_tariffs";
        try (Connection conn = DBConnectionHelper.getConnection();
             Statement  st   = conn.createStatement();
             ResultSet  rs   = st.executeQuery(sql)) {
            while (rs.next()) {
                result.put(rs.getString("station_id"), rs.getString("demand_level"));
            }
        }
        System.out.println("[DB:" + SERVER_NAME + "] Loaded " + result.size()
                + " tariff records.");
        return result;
    }
}
