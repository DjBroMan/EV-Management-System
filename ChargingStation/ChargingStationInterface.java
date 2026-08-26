import java.rmi.*;
import Clock.LamportResult;

// Remote interface defining methods to check charging station status and ports with Lamport timestamp propagation
public interface ChargingStationInterface extends Remote
{
    // Returns total available ports at the station
    String getStationStatus() throws RemoteException;
    LamportResult<String> getStationStatus(long clientLamport) throws RemoteException;

    // Returns list of available port IDs
    String getAvailablePorts() throws RemoteException;
    LamportResult<String> getAvailablePorts(long clientLamport) throws RemoteException;

    // Checks if a specific port (like P1, P2) is free or occupied
    String checkPortAvailability(String portId) throws RemoteException;
    LamportResult<String> checkPortAvailability(String portId, long clientLamport) throws RemoteException;

    // Reserves a specific port if it is AVAILABLE (atomic check-and-reserve)
    String reservePort(String portId) throws RemoteException;
    LamportResult<String> reservePort(String portId, long clientLamport) throws RemoteException;

    // Releases a port (RESERVED or CHARGING) back to AVAILABLE
    String releasePort(String portId) throws RemoteException;
    LamportResult<String> releasePort(String portId, long clientLamport) throws RemoteException;

    // Atomically finds and reserves the first AVAILABLE port.
    String reserveAnyAvailablePort() throws RemoteException;
    LamportResult<String> reserveAnyAvailablePort(long clientLamport) throws RemoteException;

    // Transitions a RESERVED port into CHARGING.
    String startPortCharging(String portId) throws RemoteException;
    LamportResult<String> startPortCharging(String portId, long clientLamport) throws RemoteException;

    // Triggers Cristian physical clock synchronization on demand
    String synchronizeClock() throws RemoteException;
}