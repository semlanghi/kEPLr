package org.apache.kafka.streams.keplr.operators.statestore_non_interval;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.apache.kafka.streams.state.internals.WrappedStateStore;

/**
 * Wrapper for the {@link FollowedByEventStore} that provides serialization and deserialization functions.
 *
 * @see Serde
 * @see StateSerdes
 * @param <K>
 * @param <V>
 */

public class WrappedFollowedByStore<K,V>
        extends WrappedStateStore<FollowedByEventStore<Bytes, byte[]>, TypedKey<K>, V>
        implements FollowedByEventStore<TypedKey<K>, V> {

    private final long windowSizeMs;
    private final String metricScope;
    private final Time time;
    final Serde<TypedKey<K>> keySerde;
    final Serde<V> valueSerde;
    StateSerdes<TypedKey<K>, V> serdes;

    @Override
    public void init(ProcessorContext context, StateStore root) {
        super.init(context, root);
        serdes = new StateSerdes<TypedKey<K>, V>(
                ProcessorStateManager.storeChangelogTopic("TEST", name()),
                keySerde == null ? (Serde<TypedKey<K>>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);
    }

    public WrappedFollowedByStore(FollowedByEventStore<Bytes,byte[]> bytesWindowStore, long windowSize, String metricsScope, Time time, Serde<TypedKey<K>> keySerde, Serde<V> valueSerde) {
        super(bytesWindowStore);
        this.time=time;
        this.metricScope = metricsScope;
        this.windowSizeMs = windowSize;
        this.keySerde = keySerde;
        this.valueSerde=valueSerde;
    }




    private Bytes keyBytes(final TypedKey<K> key) {
        return Bytes.wrap(serdes.rawKey(key));
    }




    @Override
    public void putIntervalEvent(TypedKey<K> key, V value, long timestamp, boolean allowOverlaps) {
        wrapped().putIntervalEvent(keyBytes(key), serdes.rawValue(value), timestamp, allowOverlaps);
    }





    @Override
    public KeyValueIterator<TypedKey<K>, V> fetchEventsInLeft(TypedKey<K> key, long timestamp, boolean delete) {
        return new FollowedByWrapperKeyValueIterator<K,V>(
                wrapped().fetchEventsInLeft(keyBytes(key), timestamp, delete),
                serdes,
                time);
    }

    @Override
    public KeyValueIterator<TypedKey<K>, V> fetchEventsInRight(TypedKey<K> key, long timestamp) {

        return new FollowedByWrapperKeyValueIterator<K,V>(
                wrapped().fetchEventsInRight(keyBytes(key), timestamp),
                serdes,
                time);
    }


    @Override
    public void put(TypedKey<K> key, V value) {

    }

    @Override
    public void put(TypedKey<K> key, V value, long windowStartTimestamp) {

    }

    @Override
    public V fetch(TypedKey<K> key, long time) {
        return null;
    }

    @Override
    public WindowStoreIterator<V> fetch(TypedKey<K> key, long timeFrom, long timeTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<TypedKey<K>>, V> fetch(TypedKey<K> from, TypedKey<K> to, long timeFrom, long timeTo) {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<TypedKey<K>>, V> all() {
        return null;
    }

    @Override
    public KeyValueIterator<Windowed<TypedKey<K>>, V> fetchAll(long timeFrom, long timeTo) {
        return null;
    }

    private class FollowedByWrapperKeyValueIterator<K, V> implements KeyValueIterator<TypedKey<K>, V> {

        private final KeyValueIterator<Bytes, byte[]> iter;
        private final StateSerdes<TypedKey<K>, V> serdes;
        private final long startNs;
        private final Time time;

        FollowedByWrapperKeyValueIterator(final KeyValueIterator<Bytes, byte[]> iter,
                                        final StateSerdes<TypedKey<K>, V> serdes,
                                        final Time time) {
            this.iter = iter;
            this.serdes = serdes;
            this.startNs = time.nanoseconds();
            this.time = time;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<TypedKey<K>, V> next() {
            final KeyValue<Bytes, byte[]> next = iter.next();
            return KeyValue.pair(serdes.keyFrom(next.key.get()), serdes.valueFrom(next.value));
        }

        private TypedKey<K> key(final Bytes bytesKey) {
            return serdes.keyFrom(bytesKey.get());
        }

        @Override
        public void remove() {
            iter.remove();
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                //metrics.recordLatency(sensor, startNs, time.nanoseconds());
            }
        }

        @Override
        public TypedKey<K> peekNextKey() {
            return key(iter.peekNextKey());
        }
    }

    private class FollowedByWrapperKeyValueCompositeIterator<K, V> implements KeyValueIterator<TypedKey<K>, long[]> {

        private final KeyValueIterator<Bytes, long[]> iter;
        private final StateSerdes<TypedKey<K>, V> serdes;
        private final long startNs;
        private final Time time;

        FollowedByWrapperKeyValueCompositeIterator(final KeyValueIterator<Bytes, long[]> iter,
                                          final StateSerdes<TypedKey<K>, V> serdes,
                                          final Time time) {
            this.iter = iter;
            this.serdes = serdes;
            this.startNs = time.nanoseconds();
            this.time = time;
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public KeyValue<TypedKey<K>, long[]> next() {
            final KeyValue<Bytes, long[]> next = iter.next();
            return KeyValue.pair(serdes.keyFrom(next.key.get()), next.value);
        }

        private TypedKey<K> key(final Bytes bytesKey) {
            return serdes.keyFrom(bytesKey.get());
        }

        @Override
        public void remove() {
            iter.remove();
        }

        @Override
        public void close() {
            try {
                iter.close();
            } finally {
                //metrics.recordLatency(sensor, startNs, time.nanoseconds());
            }
        }

        @Override
        public TypedKey<K> peekNextKey() {
            return key(iter.peekNextKey());
        }
    }
}
