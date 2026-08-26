package Clock;

import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Dedicated RMI TimeServer providing reference physical time for Cristian's
 * algorithm.
 */
public class TimeServer extends UnicastRemoteObject implements TimeServerInterface {

    private static final long serialVersionUID = 1L;

    public TimeServer() throws RemoteException {
        super(2239);
    }

    @Override
    public long getPhysicalTimeMillis() throws RemoteException {
        return System.currentTimeMillis();
    }

    @Override
    public String getFormattedPhysicalTime() throws RemoteException {
        return PhysicalClock.getLocalPhysicalTimeFormatted();
    }

    public static void main(String[] args) {
        try {
            String rmiHost = System.getenv("RMI_SERVER_HOST");
            if (rmiHost != null && !rmiHost.trim().isEmpty()) {
                System.setProperty("java.rmi.server.hostname", rmiHost);
            }

            LocateRegistry.createRegistry(1239);

            TimeServer server = new TimeServer();

            Naming.rebind("rmi://localhost:1239/TimeServer", server);

            System.out.println("=================================");
            System.out.println("      TIME RMI SERVER (REFERENCE)");
            System.out.println("=================================");
            System.out.println("TimeServer started successfully.");
            System.out.println("Registry Port: 1239");
            System.out.println("Exported Port: 2239");
            System.out.println("Service Name: TimeServer");
            System.out.println("Reference Physical Time: " + PhysicalClock.getLocalPhysicalTimeFormatted());
            System.out.println("Waiting for synchronization requests...");
            System.out.println("=================================");

        } catch (Exception e) {
            System.out.println("TimeServer Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
