import java.rmi.Remote;
import java.rmi.RemoteException;

// Remote interface defining methods for calculating charging price and demand multipliers
public interface PricingInterface extends Remote {

    // Calculates total cost based on station ID demand and energy consumed in kWh
    double calculatePrice(
            String stationId,
            double energyConsumed
    ) throws RemoteException;

    // Returns demand multiplier for a specific station (LOW, MEDIUM, HIGH)
    double getDemandMultiplier(
            String stationId
    ) throws RemoteException;
}
