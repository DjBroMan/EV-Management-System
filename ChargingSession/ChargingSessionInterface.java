import java.rmi.*;
import Clock.LamportResult;

// Remote interface for managing charging sessions with Lamport timestamp propagation
public interface ChargingSessionInterface extends Remote {

    // Start a charging session using a reservation ID
    String startCharging(String reservationId) throws RemoteException;
    LamportResult<String> startCharging(String reservationId, long clientLamport) throws RemoteException;

    // Stop an active charging session
    String stopCharging(String sessionId) throws RemoteException;
    LamportResult<String> stopCharging(String sessionId, long clientLamport) throws RemoteException;

    // Get current status of a session
    String getSessionStatus(String sessionId) throws RemoteException;
    LamportResult<String> getSessionStatus(String sessionId, long clientLamport) throws RemoteException;

    // Get energy consumed (kWh) for a session as a number.
    double getEnergyConsumed(String sessionId) throws RemoteException;
    LamportResult<Double> getEnergyConsumed(String sessionId, long clientLamport) throws RemoteException;

    // Get charging port ID associated with a session.
    String getSessionPort(String sessionId) throws RemoteException;
    LamportResult<String> getSessionPort(String sessionId, long clientLamport) throws RemoteException;

    // Triggers Cristian physical clock synchronization on demand
    String synchronizeClock() throws RemoteException;
}