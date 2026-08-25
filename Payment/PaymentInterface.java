import java.rmi.*;

// Remote interface defining methods for processing payments and checking payment records
public interface PaymentInterface extends Remote {

    // Process payment for a given (completed) session ID.
    // The amount is derived via RMI from ChargingSessionServer's energy
    // consumed and PricingServer's rate, not supplied by the caller.
    String makePayment(
        String sessionId
    ) throws RemoteException;

    // Check payment status using payment ID
    String getPaymentStatus(
        String paymentId
    ) throws RemoteException;

    // Retrieve full payment breakdown by payment ID
    String getPaymentDetails(
        String paymentId
    ) throws RemoteException;
}