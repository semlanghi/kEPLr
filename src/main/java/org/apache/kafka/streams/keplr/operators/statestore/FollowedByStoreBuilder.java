package org.apache.kafka.streams.keplr.operators.statestore;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;

public class FollowedByStoreBuilder<K,V> extends AbstractStoreBuilder<TypedKey<K>, V, FollowedByEventStore<TypedKey<K>, V>> {

    private final FollowedByBytesStoreSupplier storeSupplier;


    public FollowedByStoreBuilder(final FollowedByBytesStoreSupplier storeSupplier,
                                  final Serde<TypedKey<K>> keySerde,
                                  final Serde<V> valueSerde,
                                  final Time time) {
        super(storeSupplier.name(), keySerde, valueSerde, time);
        this.storeSupplier = storeSupplier;
    }

    @Override
    public FollowedByEventStore<TypedKey<K>, V> build() {
        return new WrappedFollowedByStore<K,V>(
                storeSupplier.get(),
                storeSupplier.windowSize(),
                storeSupplier.metricsScope(),
                time,
                keySerde,
                valueSerde);
    }



}
