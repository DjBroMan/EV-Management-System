import java.rmi.Remote;
import java.rmi.RemoteException;
import Clock.LamportResult;

/**
 * Remote interface exposed by ReservationServerManager.
 * Used by ReservationServer (Primary) to dispatch replication events,
 * and by administrative/testing tools to inspect or trigger failover.
 */
public interface ReservationManagerInterface extends Remote {

    /**
     * Dispatches a new reservation state change to the secondary replica.
     */
    LamportResult<Boolean> replicateReservation(
            String reservationId,
            String reservationDetails,
            String portId,
            int currentCounter,
            long clientLamport) throws RemoteException;

    /**
     * Dispatches a reservation cancellation state change to the secondary replica.
     */
    LamportResult<Boolean> replicateCancellation(
            String reservationId,
            long clientLamport) throws RemoteException;

    /**
     * Coordinates pulling full snapshot from Primary and applying to Secondary.
     */
    LamportResult<Boolean> triggerFullSynchronization(
            long clientLamport) throws RemoteException;

    /**
     * Checks if Primary is reachable; if not, promotes Secondary to Primary.
     */
    LamportResult<Boolean> checkAndFailover(
            long clientLamport) throws RemoteException;

    /**
     * Returns human-readable status of the primary-secondary replication cluster.
     */
    LamportResult<String> getReplicationStatus(
            long clientLamport) throws RemoteException;
}
