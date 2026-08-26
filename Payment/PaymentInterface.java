import java.rmi.*;
import Clock.LamportResult;

// Remote interface defining methods for processing payments and checking payment records with Lamport timestamp propagation
public interface PaymentInterface extends Remote {

    // Process payment for a given (completed) session ID.
    String makePayment(
        String sessionId
    ) throws RemoteException;

    LamportResult<String> makePayment(
        String sessionId,
        long clientLamport
    ) throws RemoteException;

    // Check payment status using payment ID
    String getPaymentStatus(
        String paymentId
    ) throws RemoteException;

    LamportResult<String> getPaymentStatus(
        String paymentId,
        long clientLamport
    ) throws RemoteException;

    // Retrieve full payment breakdown by payment ID
    String getPaymentDetails(
        String paymentId
    ) throws RemoteException;

    LamportResult<String> getPaymentDetails(
        String paymentId,
        long clientLamport
    ) throws RemoteException;

    // Triggers Cristian physical clock synchronization on demand
    String synchronizeClock() throws RemoteException;
}