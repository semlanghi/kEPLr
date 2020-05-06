package org.apache.kafka.streams.keplr.operators.statestore;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.state.internals.AbstractStoreBuilder;

/**
 * Builder for the the {@link org.apache.kafka.streams.keplr.operators.EventOccurrenceSupplier},
 * keeping a {@link EventOccurrenceBytesStoreSupplier}, which eventually will return the {@link EventOccurrenceEventStore} instance,
 * parametrized in bytes.
 * @param <K>
 * @param <V>
 */

public class EventOccurrenceStoreBuilder<K,V> extends AbstractStoreBuilder<TypedKey<K>, V, EventOccurrenceEventStore<TypedKey<K>, V>> {

    private final EventOccurrenceBytesStoreSupplier storeSupplier;


    public EventOccurrenceStoreBuilder(final EventOccurrenceBytesStoreSupplier storeSupplier,
                                  final Serde<TypedKey<K>> keySerde,
                                  final Serde<V> valueSerde,
                                  final Time time) {
        super(storeSupplier.name(), keySerde, valueSerde, time);
        this.storeSupplier = storeSupplier;
    }

    @Override
    public EventOccurrenceEventStore<TypedKey<K>, V> build() {
        return new WrappedEventOccurrenceStore<K,V>(
                storeSupplier.get(),
                storeSupplier.windowSize(),
                storeSupplier.metricsScope(),
                time,
                keySerde,
                valueSerde);
    }

}
