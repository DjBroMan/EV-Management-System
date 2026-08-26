package Clock;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Remote interface for the reference TimeServer used by Cristian's algorithm.
 */
public interface TimeServerInterface extends Remote {

    /**
     * Returns reference physical time of the TimeServer in milliseconds since epoch.
     */
    long getPhysicalTimeMillis() throws RemoteException;

    /**
     * Returns reference physical time of the TimeServer as formatted string.
     */
    String getFormattedPhysicalTime() throws RemoteException;
}
