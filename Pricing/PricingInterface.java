import java.rmi.Remote;
import java.rmi.RemoteException;
import Clock.LamportResult;

// Remote interface defining methods for calculating charging price and demand multipliers with Lamport timestamp propagation
public interface PricingInterface extends Remote {

    // Calculates total cost based on station ID demand and energy consumed in kWh
    double calculatePrice(
            String stationId,
            double energyConsumed
    ) throws RemoteException;

    LamportResult<Double> calculatePrice(
            String stationId,
            double energyConsumed,
            long clientLamport
    ) throws RemoteException;

    // Returns demand multiplier for a specific station (LOW, MEDIUM, HIGH)
    double getDemandMultiplier(
            String stationId
    ) throws RemoteException;

    LamportResult<Double> getDemandMultiplier(
            String stationId,
            long clientLamport
    ) throws RemoteException;

    // Triggers Cristian physical clock synchronization on demand
    String synchronizeClock() throws RemoteException;
}
