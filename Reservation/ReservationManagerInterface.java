import java.rmi.RemoteException;
import Clock.LamportResult;

/**
 * Remote interface exposed by ReservationServerManager.
 * Extends ReservationInterface so the Manager can act as the single entry point proxy
 * for EVClient, while also providing replication dispatch, full synchronization,
 * health checks, and failover management.
 */
public interface ReservationManagerInterface extends ReservationInterface {

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
     * Checks if Primary is reachable; if not, promotes Secondary to Primary and routes traffic.
     */
    LamportResult<Boolean> checkAndFailover(
            long clientLamport) throws RemoteException;

    /**
     * Returns human-readable status of the primary-secondary replication cluster.
     */
    LamportResult<String> getReplicationStatus(
            long clientLamport) throws RemoteException;
}

