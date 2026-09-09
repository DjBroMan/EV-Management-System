import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializable DTO representing a full point-in-time snapshot
 * of the in-memory reservation state for primary-backup replication.
 */
public class ReservationStateSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, String> reservations;
    private final Map<String, String> reservationPorts;
    private final int reservationCounter;

    public ReservationStateSnapshot(
            Map<String, String> reservations,
            Map<String, String> reservationPorts,
            int reservationCounter) {
        this.reservations = new HashMap<>(reservations);
        this.reservationPorts = new HashMap<>(reservationPorts);
        this.reservationCounter = reservationCounter;
    }

    public Map<String, String> getReservations() {
        return reservations;
    }

    public Map<String, String> getReservationPorts() {
        return reservationPorts;
    }

    public int getReservationCounter() {
        return reservationCounter;
    }

    @Override
    public String toString() {
        return "ReservationStateSnapshot [Total Reservations=" + reservations.size()
                + ", ReservationCounter=" + reservationCounter + "]";
    }
}
