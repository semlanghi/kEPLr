package org.apache.kafka.streams.keplr.operators.statestore;

import org.apache.kafka.streams.state.KeyValueIterator;

public interface EventOccurrenceEventStore<K,V> extends EventStore<K,V> {

    public void putCompositeEvent(K key, long[] timestamps);

    public KeyValueIterator<K,long[]> fetchEvents(K key, long timestamp);

    public KeyValueIterator<K,V> retrieveEvents(K key, long[] timestamps);
}
