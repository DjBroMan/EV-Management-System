import java.rmi.*;

// Remote interface defining methods to check charging station status and ports
public interface ChargingStationInterface extends Remote
{
    // Returns total available ports at the station
    String getStationStatus() throws RemoteException;

    // Returns list of available port IDs
    String getAvailablePorts() throws RemoteException;

    // Checks if a specific port (like P1, P2) is free or occupied
    String checkPortAvailability(String portId) throws RemoteException;

    // Reserves a specific port if it is AVAILABLE (atomic check-and-reserve)
    String reservePort(String portId) throws RemoteException;

    // Releases a port (RESERVED or CHARGING) back to AVAILABLE
    String releasePort(String portId) throws RemoteException;

    // Atomically finds and reserves the first AVAILABLE port.
    // Used by ReservationServer so callers don't need to name a port.
    // Returns the reserved port ID, or "NONE" if no port is available.
    String reserveAnyAvailablePort() throws RemoteException;

    // Transitions a RESERVED port into CHARGING. Used by ChargingSessionServer.
    // Returns "CHARGING_STARTED", "PORT_NOT_FOUND", or "PORT_NOT_RESERVED".
    String startPortCharging(String portId) throws RemoteException;
}