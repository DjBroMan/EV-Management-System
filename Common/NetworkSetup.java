package Common;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.rmi.server.RMISocketFactory;

/**
 * Installs an RMISocketFactory whose outgoing connect phase is bounded to a
 * few seconds instead of relying on OS TCP defaults. Discovered live in
 * Docker: connecting to a `docker stop`-ed (but not yet removed) container
 * still attached to the bridge network can silently hang far longer than a
 * healthy RMI call ever would, because the veth/network teardown timing is
 * not fully deterministic relative to the stop command returning. Any
 * component that iterates several peers sequentially -- most notably the
 * Manager's background health/leader-discovery tick, and each cluster's own
 * Bully heartbeat loop -- must not let one dead peer's slow connect attempt
 * push back every subsequent check.
 */
public class NetworkSetup {

    private static final int CONNECT_TIMEOUT_MS = 2000;

    public static void installBoundedConnectTimeout() {
        try {
            RMISocketFactory.setSocketFactory(new RMISocketFactory() {
                @Override
                public Socket createSocket(String host, int port) throws IOException {
                    Socket socket = new Socket();
                    socket.connect(new InetSocketAddress(host, port), CONNECT_TIMEOUT_MS);
                    return socket;
                }
                @Override
                public ServerSocket createServerSocket(int port) throws IOException {
                    return new ServerSocket(port);
                }
            });
        } catch (IOException e) {
            System.out.println("WARNING: could not install bounded-connect-timeout socket factory: " + e.getMessage());
        }
    }
}
