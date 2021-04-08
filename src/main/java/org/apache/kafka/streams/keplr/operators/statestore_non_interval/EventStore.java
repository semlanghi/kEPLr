package org.apache.kafka.streams.keplr.operators.statestore_non_interval;

import org.apache.kafka.streams.state.WindowStore;

/**
 * A store for keeping events in the form of key-value pairs. Giving the possibility to specify also
 * an interval by the a start and end parameters.
 * @param <K>
 * @param <V>
 */
public interface EventStore<K,V> extends WindowStore<K,V>{

    public void putEvent(K key, V value, long timestamp, boolean allowOverlaps);

}
