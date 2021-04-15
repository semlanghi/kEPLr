package org.apache.kafka.streams.keplr.operators.statestore_non_interval;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.internals.InMemoryWindowBytesStoreSupplier;

public class FollowedByBytesStoreSupplierNew extends InMemoryWindowBytesStoreSupplier {

    private final long numberPreds;
    private final long withinMs;

    public FollowedByBytesStoreSupplierNew(String name, long retentionPeriod, long windowSize, boolean retainDuplicates,
                                           long numberPreds, long withinMs) {
        super(name, retentionPeriod, windowSize, retainDuplicates);
        this.numberPreds = numberPreds;
        this.withinMs=withinMs;
    }

    public FollowedByBytesStoreSupplierNew(String name, long windowSize, long withinMs) {
        super(name, 60L, 5L, false);
        this.withinMs = withinMs;
        this.numberPreds = 1L;
    }

    @Override
    public FollowedByEventStoreNew<Bytes, byte[]> get() {
        return new FollowedByStoreNew(
                name(),
                metricsScope(),
                retentionPeriod(),
                windowSize(),
                retainDuplicates(),
                numberPreds, 0, withinMs);
    }
}
