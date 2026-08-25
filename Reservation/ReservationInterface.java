import java.rmi.Remote;
import java.rmi.RemoteException;

// Remote interface defining methods for slot booking, cancellation, and retrieval
public interface ReservationInterface extends Remote {

    // Reserve a slot for a user ID and vehicle ID
    String reserveSlot(String userId, String vehicleId) throws RemoteException;

    // Cancel an existing reservation using reservation ID
    String cancelReservation(String reservationId) throws RemoteException;

    // Get reservation details using reservation ID
    String getReservation(String reservationId) throws RemoteException;

    // Get the charging port ID assigned to a reservation, or "NONE" if not found.
    // Used by ChargingSessionServer to locate the port for a reservation.
    String getReservationPort(String reservationId) throws RemoteException;
}