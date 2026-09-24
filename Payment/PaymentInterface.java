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

    // Credit a user's wallet balance
    String addFunds(
        String userId,
        double amount
    ) throws RemoteException;

    LamportResult<String> addFunds(
        String userId,
        double amount,
        long clientLamport
    ) throws RemoteException;

    // Read-only wallet balance check
    double getWalletBalance(
        String userId
    ) throws RemoteException;

    LamportResult<Double> getWalletBalance(
        String userId,
        long clientLamport
    ) throws RemoteException;

    // Atomic check-and-deduct used by ChargingSession's mid-session billing cycle.
    // Returns "OK|<newBalance>" or "INSUFFICIENT|<currentBalance>".
    String checkAndDeductBalance(
        String userId,
        double amount,
        String sessionId
    ) throws RemoteException;

    LamportResult<String> checkAndDeductBalance(
        String userId,
        double amount,
        String sessionId,
        long clientLamport
    ) throws RemoteException;
}