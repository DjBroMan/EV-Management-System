package Common;

import java.rmi.Naming;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import Clock.LogicalClock;

/**
 * Generic Bully leader-election engine, instantiated independently inside
 * EVERY cluster (one BullyElection object per server process; Reservation's
 * R1/R2/R3, Pricing's P1/P2/P3, etc. never share state or messages with each
 * other's elections -- there is no single/global election anywhere).
 *
 * Standard Bully algorithm:
 *  - A node that suspects the coordinator is down sends ELECTION to every
 *    peer with a HIGHER id.
 *  - Any higher-id peer that is alive replies OK, then starts its own
 *    election.
 *  - If no OK is received within the timeout, the node declares itself
 *    coordinator and sends COORDINATOR to every peer.
 *  - A node that receives ELECTION from a lower id replies OK and (if not
 *    already electing) starts its own election.
 *  - A node that receives COORDINATOR adopts the sender as the new leader.
 *
 * This class only implements the algorithm's control flow and peer
 * messaging; it knows nothing about reservations/ports/sessions/etc. Each
 * server wires it up with: its own id, its peer list, a lookup for peers'
 * ClusterNodeInterface, and two callbacks (become coordinator / learn who
 * the coordinator is).
 */
public class BullyElection {

    private static final long OK_WAIT_MS = 2000;
    private static final long COORDINATOR_WAIT_MS = 3000;
    private static final long HEARTBEAT_INTERVAL_MS = 3000;
    private static final int MAX_MISSED_HEARTBEATS = 2;

    private final int myId;
    private final String serviceName;
    private final List<PeerHandle> peers; // does not include self
    private final LogicalClock logicalClock;
    private final BiConsumer<String, String> log; // (eventType, message)
    private final Runnable onBecomeCoordinator;
    private final Consumer<PeerHandle> onLearnCoordinator; // called with the new leader's PeerHandle (id/host/port)

    // 4 threads: headroom so the periodic heartbeat tick is never starved by
    // the now-retrying (up to a few seconds each) election/coordinator-broadcast
    // tasks to multiple peers running concurrently on the same pool.
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
    private final Object electionLock = new Object();

    private volatile boolean electionInProgress = false;
    private volatile int currentCoordinatorId = -1;
    private volatile PeerHandle currentCoordinator = null;
    private volatile int missedHeartbeats = 0;

    public BullyElection(int myId, String serviceName, List<PeerHandle> peers,
                          LogicalClock logicalClock, BiConsumer<String, String> log,
                          Runnable onBecomeCoordinator, Consumer<PeerHandle> onLearnCoordinator) {
        this.myId = myId;
        this.serviceName = serviceName;
        this.peers = peers;
        this.logicalClock = logicalClock;
        this.log = log;
        this.onBecomeCoordinator = onBecomeCoordinator;
        this.onLearnCoordinator = onLearnCoordinator;
    }

    /** Call once at startup after the caller decides its OWN initial role/coordinator belief. */
    public void setInitialCoordinator(PeerHandle initialCoordinator) {
        if (initialCoordinator != null) {
            this.currentCoordinatorId = initialCoordinator.id;
            this.currentCoordinator = initialCoordinator;
        }
    }

    /** Starts the periodic heartbeat loop that watches the current coordinator. */
    public void startHeartbeatMonitor() {
        scheduler.scheduleWithFixedDelay(this::heartbeatTick, HEARTBEAT_INTERVAL_MS,
                HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }

    public int getCurrentCoordinatorId() {
        return currentCoordinatorId;
    }

    // ---------------------------------------------------------------
    // Heartbeat -> failure detection -> election trigger
    // ---------------------------------------------------------------

    private void heartbeatTick() {
        if (currentCoordinatorId == myId) {
            missedHeartbeats = 0;
            return; // I am the coordinator; nothing to watch
        }
        if (currentCoordinator == null) {
            startElection();
            return;
        }
        try {
            ClusterNodeInterface leader = lookupPeer(currentCoordinator);
            if (leader == null) {
                throw new Exception("lookup failed");
            }
            long sendL = logicalClock.sendEvent();
            leader.ping(sendL);
            missedHeartbeats = 0;
        } catch (Exception e) {
            missedHeartbeats++;
            log.accept("BULLY", "Heartbeat to coordinator (" + currentCoordinator.id + ") failed "
                    + missedHeartbeats + "x");
            if (missedHeartbeats >= MAX_MISSED_HEARTBEATS) {
                log.accept("BULLY", "Coordinator " + currentCoordinator.id
                        + " presumed DOWN after " + missedHeartbeats + " missed heartbeats. Starting election.");
                startElection();
            }
        }
    }

    // ---------------------------------------------------------------
    // Election initiation
    // ---------------------------------------------------------------

    public void startElection() {
        synchronized (electionLock) {
            if (electionInProgress) {
                return;
            }
            electionInProgress = true;
        }

        try {
            log.accept("BULLY", "Starting election. My ID=" + myId);
            List<PeerHandle> higher = peers.stream().filter(p -> p.id > myId).toList();

            if (higher.isEmpty()) {
                declareVictory();
                return;
            }

            CountDownLatch okLatch = new CountDownLatch(1);
            for (PeerHandle peer : higher) {
                scheduler.execute(() -> {
                    try {
                        ClusterNodeInterface node = lookupPeerWithRetry(peer, 3, 300);
                        if (node == null) return;
                        long sendL = logicalClock.sendEvent();
                        log.accept("BULLY", "Sending ELECTION to higher-ID peer " + peer.id);
                        var res = node.receiveElection(myId, sendL);
                        if (res != null && Boolean.TRUE.equals(res.getData())) {
                            logicalClock.receiveEvent(res.getTimestamp());
                            okLatch.countDown();
                        }
                    } catch (Exception ignored) {
                        // peer unreachable -- does not get to vote
                    }
                });
            }

            boolean gotOk;
            try {
                gotOk = okLatch.await(OK_WAIT_MS, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                gotOk = false;
            }

            if (!gotOk) {
                log.accept("BULLY", "No OK received from any higher-ID peer within timeout. Declaring self COORDINATOR.");
                declareVictory();
                return;
            }

            log.accept("BULLY", "Received OK from a higher-ID peer. Waiting for its COORDINATOR announcement...");
            scheduler.schedule(() -> {
                synchronized (electionLock) {
                    if (electionInProgress) {
                        log.accept("BULLY", "No COORDINATOR announcement arrived in time. Restarting election.");
                        electionInProgress = false;
                        startElection();
                    }
                }
            }, COORDINATOR_WAIT_MS, TimeUnit.MILLISECONDS);

        } finally {
            // electionInProgress is cleared either by declareVictory(), by receiveCoordinator(),
            // or by the timeout re-trigger above -- never immediately here.
        }
    }

    private void declareVictory() {
        currentCoordinatorId = myId;
        currentCoordinator = null; // "self" -- callers check getCurrentCoordinatorId()==myId
        synchronized (electionLock) {
            electionInProgress = false;
        }
        log.accept("BULLY", "No higher-ID peer responded. " + serviceName + "-" + myId + " ELECTED as new coordinator.");
        onBecomeCoordinator.run();
        broadcastCoordinator();
    }

    private void broadcastCoordinator() {
        for (PeerHandle peer : peers) {
            scheduler.execute(() -> {
                try {
                    // Retried with generous spacing: this is a one-shot, must-be-delivered
                    // message with no automatic re-send if it's lost, unlike heartbeats.
                    // A peer still working through its own startup (e.g. MySQL connection
                    // retries taking up to ~30s) must not silently miss this announcement.
                    ClusterNodeInterface node = lookupPeerWithRetry(peer, 6, 1000);
                    if (node == null) {
                        log.accept("BULLY", "WARNING: Could not deliver COORDINATOR announcement to "
                                + serviceName + "-" + peer.id + " after retries -- it will discover "
                                + "the new leader on its own next election/heartbeat cycle instead.");
                        return;
                    }
                    long sendL = logicalClock.sendEvent();
                    node.receiveCoordinator(myId, selfHost(), selfPort(), sendL);
                    log.accept("BULLY", "Sent COORDINATOR announcement to " + serviceName + "-" + peer.id);
                } catch (Exception ignored) {
                    // peer unreachable, will learn the new leader on its own next heartbeat/election
                }
            });
        }
    }

    // overridden via setters below so BullyElection doesn't need to guess its own bind host/port
    private volatile String selfHostValue = "localhost";
    private volatile int selfPortValue = 0;
    public void setSelfEndpoint(String host, int registryPort) {
        this.selfHostValue = host;
        this.selfPortValue = registryPort;
    }
    private String selfHost() { return selfHostValue; }
    private int selfPort() { return selfPortValue; }

    // ---------------------------------------------------------------
    // Incoming message handlers (called by the server's RMI methods)
    // ---------------------------------------------------------------

    /** Received an ELECTION message from a lower-id candidate. */
    public boolean handleElection(int candidateId) {
        log.accept("BULLY", "Received ELECTION from candidate " + candidateId + " (lower ID). Replying OK and starting my own election.");
        scheduler.execute(this::startElection);
        return true; // "OK, back off, I'm alive and outrank you"
    }

    /** Received an OK from a higher-id peer (used only for logging; the CountDownLatch in startElection() drives control flow). */
    public void handleOk(int fromId) {
        log.accept("BULLY", "Received OK from peer " + fromId);
    }

    /** Received a COORDINATOR announcement. */
    public void handleCoordinator(int leaderId, String leaderHost, int leaderPort) {
        synchronized (electionLock) {
            electionInProgress = false;
        }
        currentCoordinatorId = leaderId;
        currentCoordinator = (leaderId == myId) ? null : new PeerHandle(leaderId, leaderHost, leaderPort);
        missedHeartbeats = 0;
        log.accept("BULLY", (serviceName + "-" + leaderId) + " announced itself as the new COORDINATOR.");
        if (leaderId != myId) {
            onLearnCoordinator.accept(currentCoordinator);
        }
    }

    // ---------------------------------------------------------------
    // RMI lookup helper
    // ---------------------------------------------------------------

    private ClusterNodeInterface lookupPeer(PeerHandle peer) {
        try {
            String url = "rmi://" + peer.host + ":" + peer.registryPort + "/" + serviceName + "-" + peer.id;
            return (ClusterNodeInterface) Naming.lookup(url);
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Same lookup, but retried a few times with a short pause. Used only for
     * one-shot, must-be-delivered messages (COORDINATOR announcement, an
     * ELECTION send) where a peer that is still finishing its own slow
     * startup (e.g. waiting out MySQL connection retries) would otherwise
     * silently never receive the message -- a fire-and-forget single lookup
     * has no way to retry after this method returns, unlike the periodic
     * heartbeat ping which naturally retries every cycle and so does not use
     * this slower path.
     */
    private ClusterNodeInterface lookupPeerWithRetry(PeerHandle peer, int attempts, long delayMs) {
        for (int i = 0; i < attempts; i++) {
            ClusterNodeInterface node = lookupPeer(peer);
            if (node != null) return node;
            if (i < attempts - 1) {
                try { Thread.sleep(delayMs); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return null; }
            }
        }
        return null;
    }
}
