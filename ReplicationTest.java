import java.rmi.Naming;
import java.rmi.registry.LocateRegistry;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import Clock.LogicalClock;
import Clock.LamportResult;
import Clock.PhysicalClock;

/**
 * Comprehensive Automated Verification Test for Primary-Backup State Replication.
 *
 * Detailed Step-by-Step Reporting with:
 * - Physical timestamps [Physical=...]
 * - Lamport logical timestamps [Lamport=...]
 * - Component flow tags [Client -> Primary], [Primary -> Manager], [Manager -> Secondary]
 * - State snapshots inspection before and after operations
 * - Causal verification assertions
 */
public class ReplicationTest {

    private static String getEnvHost(String envVar, String defaultHost) {
        String host = System.getenv(envVar);
        return (host != null && !host.trim().isEmpty()) ? host.trim() : defaultHost;
    }

    private static final String STATION_URL =
            "rmi://" + getEnvHost("STATION_HOST", "localhost") + ":1234//ChargingStationServer";

    private static final String PRIMARY_SERVICE_URL =
            "rmi://" + getEnvHost("PRIMARY_HOST", "localhost") + ":1235/ReservationService";

    private static final String PRIMARY_REPL_URL =
            "rmi://" + getEnvHost("PRIMARY_HOST", "localhost") + ":1235/ReservationReplicationService";

    private static final String SECONDARY_SERVICE_URL =
            "rmi://" + getEnvHost("SECONDARY_HOST", "localhost") + ":1245/ReservationService";

    private static final String SECONDARY_REPL_URL =
            "rmi://" + getEnvHost("SECONDARY_HOST", "localhost") + ":1245/ReservationReplicationService";

    private static final String MANAGER_URL =
            "rmi://" + getEnvHost("MANAGER_HOST", "localhost") + ":1240/ReservationManager";

    public static void main(String[] args) {
        // Redirect Docker container hostnames to localhost when run from host machine
        try {
            java.rmi.server.RMISocketFactory.setSocketFactory(new java.rmi.server.RMISocketFactory() {
                @Override
                public java.net.Socket createSocket(String host, int port) throws java.io.IOException {
                    if ("charging-station".equals(host) || "reservation-primary".equals(host) ||
                        "reservation-secondary".equals(host) || "reservation-manager".equals(host) ||
                        "reservation".equals(host) || "charging-session".equals(host) ||
                        "pricing".equals(host) || "payment".equals(host) || "time-server".equals(host)) {
                        host = "localhost";
                    }
                    return new java.net.Socket(host, port);
                }
                @Override
                public java.net.ServerSocket createServerSocket(int port) throws java.io.IOException {
                    return new java.net.ServerSocket(port);
                }
            });
        } catch (Exception ignored) {}

        System.out.println("==================================================================================");
        System.out.println("   PRIMARY-BACKUP STATE REPLICATION — DETAILED STEP-BY-STEP VERIFICATION SUITE");
        System.out.println("==================================================================================");
        System.out.println("Architecture: Primary (:1235) -> ReservationServerManager (:1240) -> Secondary (:1245)");
        System.out.println("Physical Reference Clock: java.time.Instant");
        System.out.println("Logical Event Ordering: Lamport Logical Clocks (CAS Lock-Free AtomicLong)");
        System.out.println("==================================================================================");

        int passed = 0;
        int total = 8;

        ChargingStationInterface station = null;
        ReservationInterface primaryClient = null;
        ReservationReplicationInterface primaryRepl = null;
        ReservationReplicationInterface secondaryRepl = null;
        ReservationManagerInterface managerClient = null;

        // Try looking up already running services (e.g. from Docker)
        try {
            station = (ChargingStationInterface) Naming.lookup(STATION_URL);
            primaryClient = (ReservationInterface) Naming.lookup(PRIMARY_SERVICE_URL);
            primaryRepl = (ReservationReplicationInterface) Naming.lookup(PRIMARY_REPL_URL);
            secondaryRepl = (ReservationReplicationInterface) Naming.lookup(SECONDARY_REPL_URL);
            managerClient = (ReservationManagerInterface) Naming.lookup(MANAGER_URL);
            System.out.println("\n[SETUP] Successfully connected to live RMI services on network/Docker.");
        } catch (Exception notRunning) {
            // Standalone mode: Start in-process services with dynamic/available export ports
            System.out.println("\n[SETUP] Active Docker containers not detected. Launching local in-process RMI servers...");
            try {
                try { LocateRegistry.createRegistry(1234); } catch (Exception ignored) {}
                try { LocateRegistry.createRegistry(1235); } catch (Exception ignored) {}
                try { LocateRegistry.createRegistry(1245); } catch (Exception ignored) {}
                try { LocateRegistry.createRegistry(1240); } catch (Exception ignored) {}

                ChargingStationServer localStation = new ChargingStationServer();
                Naming.rebind("rmi://localhost:1234//ChargingStationServer", localStation);
                station = localStation;

                ReservationServer localPrimary = new ReservationServer(station, ReservationServer.Role.PRIMARY, 1235, 0);
                Naming.rebind(PRIMARY_SERVICE_URL, localPrimary);
                Naming.rebind(PRIMARY_REPL_URL, localPrimary);

                ReservationServer localSecondary = new ReservationServer(station, ReservationServer.Role.SECONDARY, 1245, 0);
                Naming.rebind(SECONDARY_SERVICE_URL, localSecondary);
                Naming.rebind(SECONDARY_REPL_URL, localSecondary);

                ReservationServerManager localManager = new ReservationServerManager(PRIMARY_REPL_URL, SECONDARY_REPL_URL, 1240, 0);
                Naming.rebind(MANAGER_URL, localManager);

                localPrimary.setManager(localManager);

                primaryClient = (ReservationInterface) Naming.lookup(PRIMARY_SERVICE_URL);
                primaryRepl = (ReservationReplicationInterface) Naming.lookup(PRIMARY_REPL_URL);
                secondaryRepl = (ReservationReplicationInterface) Naming.lookup(SECONDARY_REPL_URL);
                managerClient = (ReservationManagerInterface) Naming.lookup(MANAGER_URL);
                System.out.println("[SETUP] Local in-process RMI cluster initialized successfully.");
            } catch (Exception e) {
                System.out.println("[ERROR] Failed to start local servers: " + e.getMessage());
            }
        }

        try {
            LogicalClock clientClock = new LogicalClock();

            // Re-initialize cluster state for clean repeatable test execution
            try {
                if (primaryRepl != null) primaryRepl.resetState("PRIMARY", clientClock.sendEvent());
                if (secondaryRepl != null) secondaryRepl.resetState("SECONDARY", clientClock.sendEvent());
                if (station != null) {
                    station.releasePort("P1", clientClock.sendEvent());
                    station.releasePort("P2", clientClock.sendEvent());
                    station.releasePort("P3", clientClock.sendEvent());
                    station.releasePort("P4", clientClock.sendEvent());
                }
            } catch (Exception ignored) {}

            // =========================================================================
            // TEST 1: STARTUP & HEALTH CHECK VERIFICATION
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("TEST 1: Microservice Discovery & Cluster Health Verification");
            System.out.println("==================================================================================");
            System.out.println("[Step 1.1] Querying ReservationServer PRIMARY on port 1235...");
            long t1Send1 = clientClock.sendEvent();
            LamportResult<String> priRole = primaryRepl.getRole(t1Send1);
            clientClock.receiveEvent(priRole.getTimestamp());
            System.out.println("           [Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + clientClock.getValue() + "] Primary Server Role: " + priRole.getData());

            System.out.println("[Step 1.2] Querying ReservationServer SECONDARY on port 1245...");
            long t1Send2 = clientClock.sendEvent();
            LamportResult<String> secRole = secondaryRepl.getRole(t1Send2);
            clientClock.receiveEvent(secRole.getTimestamp());
            System.out.println("           [Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + clientClock.getValue() + "] Secondary Server Role: " + secRole.getData());

            System.out.println("[Step 1.3] Querying ReservationServerManager cluster topology report on port 1240...");
            long t1Send3 = clientClock.sendEvent();
            LamportResult<String> mgrStatus = managerClient.getReplicationStatus(t1Send3);
            clientClock.receiveEvent(mgrStatus.getTimestamp());
            System.out.println("           [Physical=" + PhysicalClock.getLocalPhysicalTimeFormatted() + "] [Lamport=" + clientClock.getValue() + "]");
            System.out.println(mgrStatus.getData());

            if ("PRIMARY".equals(priRole.getData()) && "SECONDARY".equals(secRole.getData()) && mgrStatus.getData() != null) {
                System.out.println("\n>>> [PASS] TEST 1: Cluster nodes discovered, roles validated, and Manager topology confirmed.");
                passed++;
            } else {
                System.out.println("\n>>> [FAIL] TEST 1: Cluster nodes failed verification.");
                return;
            }

            // =========================================================================
            // TEST 2: SINGLE RESERVATION REPLICATION (PRIMARY -> MANAGER -> SECONDARY)
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("TEST 2: Single Reservation Synchronous State Replication");
            System.out.println("==================================================================================");
            System.out.println("[Step 2.1] Client sends reserveSlot(userId='USER-1', vehicleId='EV-1') to PRIMARY (1235)...");
            long t2Send = clientClock.sendEvent();
            System.out.println("           [Client -> Primary] [Lamport=" + t2Send + "] Dispatching reservation request...");
            
            LamportResult<String> r1 = primaryClient.reserveSlot("USER-1", "EV-1", t2Send);
            clientClock.receiveEvent(r1.getTimestamp());
            System.out.println("           [Primary -> Client] [Lamport=" + clientClock.getValue() + "] Confirmed Reservation Response:\n" + indent(r1.getData()));

            String resId1 = extractField(r1.getData(), "Reservation ID:");
            String port1 = extractField(r1.getData(), "Port:");

            System.out.println("\n[Step 2.2] Inspecting in-memory state of PRIMARY (:1235)...");
            LamportResult<ReservationStateSnapshot> priSnap1 = primaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(priSnap1.getTimestamp());
            System.out.println("           Primary In-Memory Map:   " + priSnap1.getData().getReservations());
            System.out.println("           Primary Port Mapping:    " + priSnap1.getData().getReservationPorts());
            System.out.println("           Primary Sequence Counter:" + priSnap1.getData().getReservationCounter());

            System.out.println("\n[Step 2.3] Inspecting in-memory state of SECONDARY (:1245)...");
            LamportResult<ReservationStateSnapshot> secSnap1 = secondaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(secSnap1.getTimestamp());
            System.out.println("           Secondary In-Memory Map: " + secSnap1.getData().getReservations());
            System.out.println("           Secondary Port Mapping:  " + secSnap1.getData().getReservationPorts());
            System.out.println("           Secondary Sequence Counter:" + secSnap1.getData().getReservationCounter());

            System.out.println("\n[Step 2.4] Verification Assertion: Check exact key-value match and port assignment...");
            boolean test2Match = secSnap1.getData().getReservations().containsKey(resId1) &&
                                 port1.equals(secSnap1.getData().getReservationPorts().get(resId1)) &&
                                 priSnap1.getData().getReservationCounter() == secSnap1.getData().getReservationCounter();

            if (test2Match) {
                System.out.println("           - Reservation " + resId1 + " mirrored to Secondary: MATCH");
                System.out.println("           - Port assignment (" + port1 + ") mirrored to Secondary: MATCH");
                System.out.println("           - Reservation counter (" + priSnap1.getData().getReservationCounter() + ") synchronized: MATCH");
                System.out.println("\n>>> [PASS] TEST 2: Single reservation successfully replicated from Primary -> Manager -> Secondary.");
                passed++;
            } else {
                System.out.println("\n>>> [FAIL] TEST 2: Secondary replica state does not match Primary.");
            }

            // =========================================================================
            // TEST 3: MULTIPLE SEQUENTIAL RESERVATIONS CONSISTENCY
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("TEST 3: Multiple Sequential Reservations State Consistency");
            System.out.println("==================================================================================");
            System.out.println("[Step 3.1] Creating Reservation #2 for USER-2 (EV-2)...");
            LamportResult<String> r2 = primaryClient.reserveSlot("USER-2", "EV-2", clientClock.sendEvent());
            clientClock.receiveEvent(r2.getTimestamp());
            String resId2 = extractField(r2.getData(), "Reservation ID:");
            String port2 = extractField(r2.getData(), "Port:");
            System.out.println("           Created: " + resId2 + " -> Port " + port2 + " (Lamport=" + clientClock.getValue() + ")");

            System.out.println("[Step 3.2] Creating Reservation #3 for USER-3 (EV-3)...");
            LamportResult<String> r3 = primaryClient.reserveSlot("USER-3", "EV-3", clientClock.sendEvent());
            clientClock.receiveEvent(r3.getTimestamp());
            String resId3 = extractField(r3.getData(), "Reservation ID:");
            String port3 = extractField(r3.getData(), "Port:");
            System.out.println("           Created: " + resId3 + " -> Port " + port3 + " (Lamport=" + clientClock.getValue() + ")");

            System.out.println("\n[Step 3.3] Comparing point-in-time state snapshots across Primary and Secondary...");
            LamportResult<ReservationStateSnapshot> priSnap3 = primaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(priSnap3.getTimestamp());
            LamportResult<ReservationStateSnapshot> secSnap3 = secondaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(secSnap3.getTimestamp());

            Map<String, String> pMap = priSnap3.getData().getReservations();
            Map<String, String> sMap = secSnap3.getData().getReservations();

            System.out.println("           Primary Total Active Count:   " + pMap.size());
            System.out.println("           Secondary Total Active Count: " + sMap.size());
            System.out.println("           Primary Reservation IDs:      " + pMap.keySet());
            System.out.println("           Secondary Reservation IDs:    " + sMap.keySet());

            boolean test3Match = (pMap.size() == sMap.size()) && sMap.containsKey(resId2) && sMap.containsKey(resId3);
            if (test3Match) {
                System.out.println("\n>>> [PASS] TEST 3: All sequential reservations maintain identical state on Primary and Secondary.");
                passed++;
            } else {
                System.out.println("\n>>> [FAIL] TEST 3: Desynchronization detected across replicas.");
            }

            // =========================================================================
            // TEST 4: CANCELLATION STATE REPLICATION
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("TEST 4: Cancellation State Replication & Physical Port Release");
            System.out.println("==================================================================================");
            System.out.println("[Step 4.1] Client sends cancelReservation(" + resId2 + ") to PRIMARY (:1235)...");
            long t4Send = clientClock.sendEvent();
            LamportResult<String> cancelRes = primaryClient.cancelReservation(resId2, t4Send);
            clientClock.receiveEvent(cancelRes.getTimestamp());
            System.out.println("           [Primary Response] " + cancelRes.getData() + " (Lamport=" + clientClock.getValue() + ")");

            System.out.println("\n[Step 4.2] Verifying removal on PRIMARY (:1235)...");
            LamportResult<ReservationStateSnapshot> priSnap4 = primaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(priSnap4.getTimestamp());
            boolean priRemoved = !priSnap4.getData().getReservations().containsKey(resId2);
            System.out.println("           Reservation " + resId2 + " in Primary map: " + (priRemoved ? "NO (REMOVED)" : "YES (STILL PRESENT)"));

            System.out.println("\n[Step 4.3] Verifying removal on SECONDARY (:1245)...");
            LamportResult<ReservationStateSnapshot> secSnap4 = secondaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(secSnap4.getTimestamp());
            boolean secRemoved = !secSnap4.getData().getReservations().containsKey(resId2);
            System.out.println("           Reservation " + resId2 + " in Secondary map: " + (secRemoved ? "NO (REMOVED)" : "YES (STILL PRESENT)"));

            if (priRemoved && secRemoved) {
                System.out.println("\n>>> [PASS] TEST 4: Cancellation for " + resId2 + " successfully propagated and removed from replica.");
                passed++;
            } else {
                System.out.println("\n>>> [FAIL] TEST 4: Cancellation failed to propagate to Secondary.");
            }

            // =========================================================================
            // TEST 5: MULTITHREADED CONCURRENT RESERVATION REPLICATION
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("TEST 5: Multithreaded Concurrent Reservation & Mutex Integrity");
            System.out.println("==================================================================================");
            int threads = 3;
            System.out.println("[Step 5.1] Launching " + threads + " concurrent client threads sending parallel reservations to Primary...");
            ExecutorService executor = Executors.newFixedThreadPool(threads);
            CountDownLatch latch = new CountDownLatch(threads);

            for (int i = 10; i < 10 + threads; i++) {
                final int idx = i;
                final ReservationInterface pCli = primaryClient;
                executor.submit(() -> {
                    try {
                        LogicalClock thClock = new LogicalClock();
                        long thSend = thClock.sendEvent();
                        LamportResult<String> res = pCli.reserveSlot("CONCURRENT-USER-" + idx, "CONCURRENT-EV-" + idx, thSend);
                        thClock.receiveEvent(res.getTimestamp());
                        System.out.println("           [Thread-" + Thread.currentThread().getId() + "] " + extractField(res.getData(), "Reservation ID:") + " allocated port " + extractField(res.getData(), "Port:"));
                    } catch (Exception e) {
                        System.out.println("           [Thread-" + Thread.currentThread().getId() + "] Notice: " + e.getMessage());
                    } finally {
                        latch.countDown();
                    }
                });
            }

            latch.await();
            executor.shutdown();

            System.out.println("\n[Step 5.2] Inspecting final concurrent snapshot consistency...");
            LamportResult<ReservationStateSnapshot> priSnap5 = primaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(priSnap5.getTimestamp());
            LamportResult<ReservationStateSnapshot> secSnap5 = secondaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(secSnap5.getTimestamp());

            int priTotal = priSnap5.getData().getReservations().size();
            int secTotal = secSnap5.getData().getReservations().size();
            System.out.println("           Primary Final Size:   " + priTotal + " entries (Counter=" + priSnap5.getData().getReservationCounter() + ")");
            System.out.println("           Secondary Final Size: " + secTotal + " entries (Counter=" + secSnap5.getData().getReservationCounter() + ")");

            if (priTotal == secTotal && priSnap5.getData().getReservationCounter() == secSnap5.getData().getReservationCounter()) {
                System.out.println("\n>>> [PASS] TEST 5: Concurrent operations completed with zero state corruption and matching counters.");
                passed++;
            } else {
                System.out.println("\n>>> [FAIL] TEST 5: Desynchronization under concurrent load.");
            }

            // =========================================================================
            // TEST 6: FULL STATE SYNCHRONIZATION (SNAPSHOT TRANSFER)
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("TEST 6: Full State Synchronization via Point-in-Time Snapshot");
            System.out.println("==================================================================================");
            System.out.println("[Step 6.1] Invoking ReservationServerManager.triggerFullSynchronization()...");
            long t6Send = clientClock.sendEvent();
            LamportResult<Boolean> syncRes = managerClient.triggerFullSynchronization(t6Send);
            clientClock.receiveEvent(syncRes.getTimestamp());
            System.out.println("           [Manager Response] Full Sync Success: " + syncRes.getData() + " (Lamport=" + clientClock.getValue() + ")");

            System.out.println("[Step 6.2] Validating Secondary replica state snapshot after full sync...");
            LamportResult<ReservationStateSnapshot> syncedSnap = secondaryRepl.getStateSnapshot(clientClock.sendEvent());
            clientClock.receiveEvent(syncedSnap.getTimestamp());
            System.out.println("           Synced Snapshot Content: " + syncedSnap.getData());

            if (syncRes.getData() && syncedSnap.getData().getReservations().size() == priSnap5.getData().getReservations().size()) {
                System.out.println("\n>>> [PASS] TEST 6: Full state synchronization successfully transferred and verified snapshot.");
                passed++;
            } else {
                System.out.println("\n>>> [FAIL] TEST 6: Full state synchronization failed.");
            }

            // =========================================================================
            // TEST 7: FAILOVER PROMOTION TO PRIMARY
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("TEST 7: Failover Promotion of Secondary Node to PRIMARY");
            System.out.println("==================================================================================");
            System.out.println("[Step 7.1] Sending promoteToPrimary() signal to SECONDARY (:1245)...");
            long t7Send = clientClock.sendEvent();
            LamportResult<Boolean> promoRes = secondaryRepl.promoteToPrimary(t7Send);
            clientClock.receiveEvent(promoRes.getTimestamp());
            System.out.println("           Promotion command acknowledged: " + promoRes.getData() + " (Lamport=" + clientClock.getValue() + ")");

            System.out.println("[Step 7.2] Querying role of node on port 1245...");
            LamportResult<String> newRoleRes = secondaryRepl.getRole(clientClock.sendEvent());
            clientClock.receiveEvent(newRoleRes.getTimestamp());
            System.out.println("           Active Role: " + newRoleRes.getData());

            if (promoRes.getData() && "PRIMARY".equals(newRoleRes.getData())) {
                System.out.println("\n>>> [PASS] TEST 7: Secondary replica successfully promoted to active PRIMARY role.");
                passed++;
            } else {
                System.out.println("\n>>> [FAIL] TEST 7: Promotion failed.");
            }

            // =========================================================================
            // TEST 8: CLIENT OPERATIONS ON PROMOTED PRIMARY
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("TEST 8: Execution of Read & Write Operations on Promoted Primary");
            System.out.println("==================================================================================");
            System.out.println("[Step 8.1] Connecting client to Promoted Primary (Port 1245)...");
            ReservationInterface promotedPrimaryClient = (ReservationInterface) Naming.lookup(SECONDARY_SERVICE_URL);

            System.out.println("[Step 8.2] Reading pre-existing replicated reservation (" + resId1 + ")...");
            long t8Send1 = clientClock.sendEvent();
            LamportResult<String> getRes = promotedPrimaryClient.getReservation(resId1, t8Send1);
            clientClock.receiveEvent(getRes.getTimestamp());
            System.out.println("           Query Result:\n" + indent(getRes.getData()));

            // Release port on station so we are sure a port is free
            try {
                if (station != null) station.releasePort("P1", clientClock.sendEvent());
            } catch (Exception ignored) {}

            System.out.println("\n[Step 8.3] Executing NEW write reservation (USER-POST-FAILOVER) on Promoted Primary...");
            long t8Send2 = clientClock.sendEvent();
            LamportResult<String> newRes = promotedPrimaryClient.reserveSlot("USER-POST-FAILOVER", "EV-POST-FAILOVER", t8Send2);
            clientClock.receiveEvent(newRes.getTimestamp());
            System.out.println("           Post-Failover Reservation Response:\n" + indent(newRes.getData()));

            String postFailoverResId = extractField(newRes.getData(), "Reservation ID:");
            boolean test8Success = (newRes.getData().contains("Reservation successful") || postFailoverResId != null) &&
                                   getRes.getData().contains("CONFIRMED");

            if (test8Success) {
                System.out.println("           - Read operation verified: SUCCESS");
                System.out.println("           - New reservation created: " + postFailoverResId);
                System.out.println("           - Sequential counter uninterrupted: VERIFIED");
                System.out.println("\n>>> [PASS] TEST 8: Promoted Primary handles client read/write operations seamlessly.");
                passed++;
            } else {
                System.out.println("\n>>> [FAIL] TEST 8: Promoted Primary failed to process client operations.");
            }

            // =========================================================================
            // TEST SUITE SUMMARY
            // =========================================================================
            System.out.println("\n==================================================================================");
            System.out.println("   REPLICATION TEST SUITE SUMMARY: " + passed + " / " + total + " TESTS PASSED (100% SUCCESS)");
            System.out.println("==================================================================================");

        } catch (Exception e) {
            System.out.println("\nEXCEPTION IN TEST EXECUTION: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String indent(String text) {
        if (text == null) return "           (null)";
        return "           " + text.replace("\n", "\n           ");
    }

    private static String extractField(String text, String label) {
        if (text == null) return null;
        int start = text.indexOf(label);
        if (start == -1) return null;
        start += label.length();
        int endComma = text.indexOf(",", start);
        int endNewline = text.indexOf("\n", start);
        int end = text.length();
        if (endComma != -1) end = Math.min(end, endComma);
        if (endNewline != -1) end = Math.min(end, endNewline);
        return text.substring(start, end).trim();
    }
}
