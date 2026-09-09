import java.rmi.Remote;
import java.rmi.RemoteException;
import Clock.LamportResult;

/**
 * Dedicated RMI interface for replication operations between
 * ReservationServerManager and ReservationServer instances (Primary / Secondary).
 */
public interface ReservationReplicationInterface extends Remote {

    /**
     * Applies a replicated reservation state update to the replica.
     */
    LamportResult<Boolean> applyReservationUpdate(
            String reservationId,
            String reservationDetails,
            String portId,
            int currentCounter,
            long clientLamport) throws RemoteException;

    /**
     * Applies a replicated cancellation state update to the replica.
     */
    LamportResult<Boolean> applyCancellationUpdate(
            String reservationId,
            long clientLamport) throws RemoteException;

    /**
     * Replaces replica in-memory state with a full state snapshot from primary.
     */
    LamportResult<Boolean> synchronizeFullState(
            ReservationStateSnapshot snapshot,
            long clientLamport) throws RemoteException;

    /**
     * Retrieves a point-in-time state snapshot from this server.
     */
    LamportResult<ReservationStateSnapshot> getStateSnapshot(
            long clientLamport) throws RemoteException;

    /**
     * Health check / heartbeat.
     */
    LamportResult<Boolean> ping(
            long clientLamport) throws RemoteException;

    /**
     * Promotes a SECONDARY replica to PRIMARY role during failover.
     */
    LamportResult<Boolean> promoteToPrimary(
            long clientLamport) throws RemoteException;

    /**
     * Returns the current role of the server ("PRIMARY" or "SECONDARY").
     */
    LamportResult<String> getRole(
            long clientLamport) throws RemoteException;

    /**
     * Resets server in-memory state and restores configured role for automated testing repeatability.
     */
    LamportResult<Boolean> resetState(
            String targetRole,
            long clientLamport) throws RemoteException;
}
