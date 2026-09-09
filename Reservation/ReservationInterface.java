import java.rmi.Remote;
import java.rmi.RemoteException;
import Clock.LamportResult;

// Remote interface defining methods for slot booking, cancellation, and retrieval with Lamport timestamp propagation
public interface ReservationInterface extends Remote {

    // Reserve a slot for a user ID and vehicle ID
    String reserveSlot(String userId, String vehicleId) throws RemoteException;

    LamportResult<String> reserveSlot(String userId, String vehicleId, long clientLamport) throws RemoteException;

    // Cancel an existing reservation using reservation ID
    String cancelReservation(String reservationId) throws RemoteException;

    LamportResult<String> cancelReservation(String reservationId, long clientLamport) throws RemoteException;

    // Get reservation details using reservation ID
    String getReservation(String reservationId) throws RemoteException;

    LamportResult<String> getReservation(String reservationId, long clientLamport) throws RemoteException;

    // Get the charging port ID assigned to a reservation, or "NONE" if not found.
    String getReservationPort(String reservationId) throws RemoteException;

    LamportResult<String> getReservationPort(String reservationId, long clientLamport) throws RemoteException;

    // Triggers Cristian physical clock synchronization on demand
    String synchronizeClock() throws RemoteException;
}