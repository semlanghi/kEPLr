package org.apache.kafka.streams.keplr.operators.statestore;

import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStore;

public interface EventStore<K,V> extends WindowStore<K,V>{

    public void putIntervalEvent(K key, V value, long start, long end, boolean allowOverlaps);

}
