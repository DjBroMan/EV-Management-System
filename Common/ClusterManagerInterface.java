package Common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import Clock.LamportResult;

/**
 * Minimal admin surface the Manager exposes to every cluster instance
 * (ChargingStation/ChargingSession/Payment; Pricing has no write path) so a
 * PRIMARY can fan a single replicated change out to its peers without
 * needing to know their addresses itself -- the Manager already tracks
 * every instance of every cluster for load balancing and health checking,
 * so it is the natural place to also perform replication fan-out.
 *
 * Bound under "ClusterManager" on the Manager's own registry port.
 */
public interface ClusterManagerInterface extends Remote {
    LamportResult<Boolean> replicateUpdate(String serviceName, int originServerId, StateDelta delta, long clientLamport) throws RemoteException;
}
