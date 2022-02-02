package keplr.operators.statestore;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.internals.InMemoryWindowBytesStoreSupplier;

public class FollowedByBytesStoreSupplier extends InMemoryWindowBytesStoreSupplier {

    private final long numberPreds;
    private final long withinMs;

    public FollowedByBytesStoreSupplier(String name, long retentionPeriod, long windowSize, boolean retainDuplicates,
                                        long numberPreds, long withinMs) {
        super(name, retentionPeriod, windowSize, retainDuplicates);
        this.numberPreds = numberPreds;
        this.withinMs=withinMs;
    }

    public FollowedByBytesStoreSupplier(String name, long windowSize, long withinMs) {
        super(name, 60L, 5L, false);
        this.withinMs = withinMs;
        this.numberPreds = 1L;
    }

    @Override
    public FollowedByEventStore<Bytes, byte[]> get() {
        return new FollowedByStore(
                name(),
                metricsScope(),
                retentionPeriod(),
                windowSize(),
                retainDuplicates(),
                numberPreds, 0, withinMs);
    }
}
