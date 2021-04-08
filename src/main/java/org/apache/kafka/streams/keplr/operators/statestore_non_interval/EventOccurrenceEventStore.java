package org.apache.kafka.streams.keplr.operators.statestore_non_interval;

import org.apache.kafka.streams.state.KeyValueIterator;

/**
 * Store for the {@link org.apache.kafka.streams.keplr.operators.EventOccurrenceSupplier}. It provides a method for
 * saving the timestamp of homogeneous events, storing them by the key and their ending timestamp, in order to use
 * them for retrieving the actual values. The {@link EventOccurrenceEventStore#fetchEvents(Object, long)} method
 * retrieve those set ot timestamps for a given event. The {@link EventOccurrenceEventStore#retrieveEvents(Object, long[])}
 * method instead retrieve the event values from a set of given timestamps.
 * @param <K>
 * @param <V>
 */

public interface EventOccurrenceEventStore<K,V> extends EventStore<K,V> {

    public void putCompositeEvent(K key, long[] timestamps);

    public KeyValueIterator<K,long[]> fetchEvents(K key, long timestamp);

    public KeyValueIterator<K,V> retrieveEvents(K key, long[] timestamps);
}
