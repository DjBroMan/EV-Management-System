import java.rmi.*;
import java.rmi.server.*;
import java.rmi.registry.*;

public class ChargingStationServer
        extends UnicastRemoteObject
        implements ChargingStationInterface
{
    private String[] ports = {"P1", "P2", "P3", "P4"};

    private String[] portStatus =
            {"AVAILABLE", "AVAILABLE", "AVAILABLE", "AVAILABLE"};

    public ChargingStationServer() throws RemoteException
    {
        super(2234);
    }

    // =========================================================
    // THREAD LOGGING
    // =========================================================

    private void log(String message)
    {
        System.out.println(
                "[Thread-" +
                Thread.currentThread().getId() +
                " | " +
                Thread.currentThread().getName() +
                "] " +
                message
        );
    }

    // =========================================================
    // SIMULATED PROCESSING DELAY
    // =========================================================

    private void simulateProcessing(long milliseconds)
    {
        try
        {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();

            log("Thread interrupted during processing.");
        }
    }

    // =========================================================
    // FIND PORT
    // =========================================================

    private int indexOfPort(String portId)
    {
        for (int i = 0; i < ports.length; i++)
        {
            if (ports[i].equalsIgnoreCase(portId))
            {
                return i;
            }
        }

        return -1;
    }

    // =========================================================
    // STATION STATUS
    // =========================================================

    public synchronized String getStationStatus()
            throws RemoteException
    {
        log("GET STATION STATUS request received.");

        simulateProcessing(500);

        int availablePorts = 0;

        for (String status : portStatus)
        {
            if (status.equals("AVAILABLE"))
            {
                availablePorts++;
            }
        }

        log("Available ports: "
                + availablePorts + "/" + ports.length);

        log("GET STATION STATUS completed.");

        return "Station EV-STATION-01: "
                + availablePorts
                + " of "
                + ports.length
                + " ports available.";
    }

    // =========================================================
    // AVAILABLE PORTS
    // =========================================================

    public synchronized String getAvailablePorts()
            throws RemoteException
    {
        log("GET AVAILABLE PORTS request received.");

        simulateProcessing(500);

        String result = "Available Ports: ";

        boolean found = false;

        for (int i = 0; i < ports.length; i++)
        {
            if (portStatus[i].equals("AVAILABLE"))
            {
                result += ports[i] + " ";
                found = true;
            }
        }

        if (!found)
        {
            log("No available ports.");

            return "No ports are currently available.";
        }

        log("Available ports: " + result);

        log("GET AVAILABLE PORTS completed.");

        return result;
    }

    // =========================================================
    // CHECK PORT
    // =========================================================

    public synchronized String checkPortAvailability(
            String portId)
            throws RemoteException
    {
        log("CHECK PORT request: " + portId);

        simulateProcessing(500);

        int i = indexOfPort(portId);

        if (i == -1)
        {
            log("Port does not exist.");

            return "Port " + portId
                    + " does not exist.";
        }

        log("Port " + ports[i]
                + " status: "
                + portStatus[i]);

        return "Port " + ports[i]
                + " is "
                + portStatus[i]
                + ".";
    }

    // =========================================================
    // RESERVE PORT
    // =========================================================

    public synchronized String reservePort(
            String portId)
            throws RemoteException
    {
        log("RESERVE PORT request: " + portId);

        log("Checking port availability...");

        simulateProcessing(700);

        int i = indexOfPort(portId);

        if (i == -1)
        {
            log("Port does not exist.");

            return "Port " + portId
                    + " does not exist.";
        }

        if (!portStatus[i].equals("AVAILABLE"))
        {
            log("Port " + ports[i]
                    + " is "
                    + portStatus[i]);

            return "Port " + ports[i]
                    + " is currently "
                    + portStatus[i]
                    + " and cannot be reserved.";
        }

        log("Port available. Reserving...");

        simulateProcessing(500);

        portStatus[i] = "RESERVED";

        log("Port " + ports[i]
                + " changed to RESERVED.");

        return "Port " + ports[i]
                + " reserved successfully.";
    }

    // =========================================================
    // RELEASE PORT
    // =========================================================

    public synchronized String releasePort(
            String portId)
            throws RemoteException
    {
        log("RELEASE PORT request: " + portId);

        simulateProcessing(500);

        int i = indexOfPort(portId);

        if (i == -1)
        {
            return "Port " + portId
                    + " does not exist.";
        }

        portStatus[i] = "AVAILABLE";

        log("Port " + portId
                + " changed to AVAILABLE.");

        return "Port " + ports[i]
                + " released successfully. "
                + "Now AVAILABLE.";
    }

    // =========================================================
    // RESERVE ANY AVAILABLE PORT
    // =========================================================

    public synchronized String reserveAnyAvailablePort()
            throws RemoteException
    {
        log("RESERVE ANY AVAILABLE PORT request received.");

        log("Scanning charging ports...");

        simulateProcessing(700);

        for (int i = 0; i < ports.length; i++)
        {
            log("Checking "
                    + ports[i]
                    + " -> "
                    + portStatus[i]);

            if (portStatus[i].equals("AVAILABLE"))
            {
                log("Available port found: "
                        + ports[i]);

                log("Simulating port reservation...");

                simulateProcessing(700);

                portStatus[i] = "RESERVED";

                log("Port "
                        + ports[i]
                        + " successfully RESERVED.");

                return ports[i];
            }
        }

        log("No available ports.");

        return "NONE";
    }

    // =========================================================
    // START CHARGING
    // =========================================================

    public synchronized String startPortCharging(
            String portId)
            throws RemoteException
    {
        log("START CHARGING request: "
                + portId);

        simulateProcessing(700);

        int i = indexOfPort(portId);

        if (i == -1)
        {
            return "PORT_NOT_FOUND";
        }

        if (!portStatus[i].equals("RESERVED"))
        {
            log("Port is not RESERVED.");

            return "PORT_NOT_RESERVED";
        }

        log("Changing "
                + portId
                + " to CHARGING...");

        simulateProcessing(500);

        portStatus[i] = "CHARGING";

        log("Port "
                + portId
                + " is now CHARGING.");

        return "CHARGING_STARTED";
    }

    // =========================================================
    // MAIN
    // =========================================================

    public static void main(String[] args)
    {
        try
        {
            String rmiHost = System.getenv("RMI_SERVER_HOST");
            if (rmiHost != null && !rmiHost.trim().isEmpty())
            {
                System.setProperty("java.rmi.server.hostname", rmiHost);
            }

            final String HOST =
                    "rmi://localhost:1234//ChargingStationServer";

            LocateRegistry.createRegistry(1234);

            ChargingStationServer server =
                    new ChargingStationServer();

            Naming.bind(HOST, server);

            System.out.println(
                    "Charging Station Server is running..."
            );

            System.out.println(
                    "Station: EV-STATION-01"
            );

            System.out.println(
                    "RMI Registry running on port 1234."
            );

            System.out.println(
                    "Waiting for client requests..."
            );
        }
        catch (Exception e)
        {
            System.out.println(
                    "Server Exception: " + e
            );
        }
    }
}