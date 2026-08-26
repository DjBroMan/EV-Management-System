package Clock;

import java.io.Serializable;

/**
 * Generic serializable container for RMI responses carrying both
 * result data and the server's Lamport timestamp for causal clock synchronization.
 */
public class LamportResult<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final T data;
    private final long timestamp;

    public LamportResult(T data, long timestamp) {
        this.data = data;
        this.timestamp = timestamp;
    }

    public T getData() {
        return data;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return String.valueOf(data);
    }
}
