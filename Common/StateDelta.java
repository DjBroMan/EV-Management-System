package Common;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Generic single-change replication payload used by every cluster's
 * ClusterNodeInterface.applyUpdate(). Carries an operation tag plus a small
 * argument list, so ONE wire format can replicate one individual change
 * (a port status flip, a session start, a payment insert, ...) without a
 * bespoke DTO class per service.
 *
 * This is intentionally NOT a full-state snapshot: per the roadmap,
 * normal replication propagates individual changes; full-state sync
 * (GenericSnapshot) is reserved for disaster recovery / new-node bootstrap.
 */
public class StateDelta implements Serializable {
    private static final long serialVersionUID = 1L;

    public final String opType;
    public final Object[] args;

    public StateDelta(String opType, Object... args) {
        this.opType = opType;
        this.args = args;
    }

    @Override
    public String toString() {
        return "StateDelta[" + opType + ", args=" + Arrays.toString(args) + "]";
    }
}
