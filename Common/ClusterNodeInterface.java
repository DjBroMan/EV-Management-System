package Common;

import java.rmi.Remote;
import java.rmi.RemoteException;
import Clock.LamportResult;

/**
 * Shared RMI surface implemented by every instance of every one of the 5
 * clusters (ChargingStation, Reservation, ChargingSession, Pricing, Payment).
 * ONE interface covers both concerns so they compose per cluster instead of
 * needing 5 bespoke replication interfaces and 5 bespoke election interfaces:
 *
 *   - Replication:  applyUpdate / getStateSnapshot / synchronizeFullState
 *   - Bully election + health: receiveElection / receiveOk / receiveCoordinator
 *                              / ping / getRole / promoteToPrimary / getServerId
 *
 * ReservationServer additionally keeps implementing the original
 * ReservationReplicationInterface unchanged, so the existing 2-node
 * replication tests keep passing exactly as before; ClusterNodeInterface is
 * how the (new) 3rd Reservation node and the other 4 clusters participate in
 * Bully election, heartbeats, and Manager-mediated replication fan-out.
 */
public interface ClusterNodeInterface extends Remote {

    /** Applies one replicated individual state change sent by the Manager (fanned out from the Primary). */
    LamportResult<Boolean> applyUpdate(StateDelta delta, long clientLamport) throws RemoteException;

    /** Returns a full point-in-time snapshot of this instance's state (disaster recovery / new-node bootstrap only). */
    LamportResult<GenericSnapshot> getClusterSnapshot(long clientLamport) throws RemoteException;

    /** Replaces this instance's state wholesale from a snapshot pulled from a healthy peer. */
    LamportResult<Boolean> applyClusterSnapshot(GenericSnapshot snapshot, long clientLamport) throws RemoteException;

    /** Liveness probe used by both the cluster's own Bully heartbeat loop and the Manager's health monitor. */
    LamportResult<Boolean> ping(long clientLamport) throws RemoteException;

    /** Called on the Bully-elected coordinator so it starts accepting client writes as PRIMARY. */
    LamportResult<Boolean> promoteToPrimary(long clientLamport) throws RemoteException;

    /** Returns "PRIMARY" or "SECONDARY". */
    LamportResult<String> getRole(long clientLamport) throws RemoteException;

    /** This instance's own numeric Bully server ID (so callers don't need a second lookup path just to learn it). */
    LamportResult<Integer> getServerId(long clientLamport) throws RemoteException;

    // ---- Bully algorithm messages ----

    /** A lower/candidate ID node is starting an election; ask this node to respond OK if it has a higher ID. */
    LamportResult<Boolean> receiveElection(int candidateId, long clientLamport) throws RemoteException;

    /** A higher-ID peer answered a candidate's ELECTION message. */
    LamportResult<Boolean> receiveOk(int fromId, long clientLamport) throws RemoteException;

    /** The winning node announces itself as the new coordinator/PRIMARY to every peer. */
    LamportResult<Boolean> receiveCoordinator(int leaderId, String leaderHost, int leaderRegistryPort, long clientLamport) throws RemoteException;
}
