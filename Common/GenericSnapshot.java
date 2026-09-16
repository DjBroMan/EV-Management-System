package Common;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Generic full point-in-time state snapshot, keyed by field name, used for
 * disaster-recovery / new-replica-bootstrap full synchronization across any
 * of the 5 clusters (ReservationStateSnapshot remains the typed snapshot
 * used by the original 2-node Reservation replication path for backward
 * compatibility; this generic form is used by the newer N-instance
 * ClusterNodeInterface path added for all 5 services).
 */
public class GenericSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Map<String, Serializable> fields;

    public GenericSnapshot() {
        this.fields = new HashMap<>();
    }

    public GenericSnapshot(Map<String, Serializable> fields) {
        this.fields = new HashMap<>(fields);
    }

    public void put(String key, Serializable value) {
        fields.put(key, value);
    }

    @SuppressWarnings("unchecked")
    public <T> T get(String key) {
        return (T) fields.get(key);
    }

    public Map<String, Serializable> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return "GenericSnapshot" + fields.keySet();
    }
}
