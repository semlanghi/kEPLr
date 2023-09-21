package keplr.operators.statestore_non_interval;

import keplr.etype.TypedKey;
import keplr.operators.IntervalFollowedBySupplier;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;

/**
 * Builder for the the {@link IntervalFollowedBySupplier},
 * keeping a {@link FollowedByBytesStoreSupplierNew}, which eventually will return the {@link FollowedByEventStoreNew} instance,
 * parametrized in bytes.
 * @param <K>
 * @param <V>
 */

public class FollowedByStoreBuilderNew<K,V> extends AbstractStoreBuilder<TypedKey<K>, V, FollowedByEventStoreNew<TypedKey<K>, V>> {

    private final FollowedByBytesStoreSupplierNew storeSupplier;


    public FollowedByStoreBuilderNew(final FollowedByBytesStoreSupplierNew storeSupplier,
                                     final Serde<TypedKey<K>> keySerde,
                                     final Serde<V> valueSerde,
                                     final Time time) {
        super(storeSupplier.name(), keySerde, valueSerde, time);
        this.storeSupplier = storeSupplier;
    }

    @Override
    public FollowedByEventStoreNew<TypedKey<K>, V> build() {
        return new WrappedFollowedByStoreNew<K,V>(
                storeSupplier.get(),
                storeSupplier.windowSize(),
                storeSupplier.metricsScope(),
                time,
                keySerde,
                valueSerde);
    }



}
