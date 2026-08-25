import java.rmi.*;

// Remote interface for managing charging sessions
public interface ChargingSessionInterface extends Remote {

    // Start a charging session using a reservation ID
    String startCharging(String reservationId) throws RemoteException;

    // Stop an active charging session
    String stopCharging(String sessionId) throws RemoteException;

    // Get current status of a session
    String getSessionStatus(String sessionId) throws RemoteException;

    // Get energy consumed (kWh) for a session as a number.
    // Used by PaymentServer to calculate the final price. Returns -1 if not found.
    double getEnergyConsumed(String sessionId) throws RemoteException;

    // Get charging port ID associated with a session.
    // Used by PaymentServer to release the port after successful payment.
    String getSessionPort(String sessionId) throws RemoteException;
}