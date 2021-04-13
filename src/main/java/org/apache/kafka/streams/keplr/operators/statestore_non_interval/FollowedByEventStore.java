package org.apache.kafka.streams.keplr.operators.statestore_non_interval;

import org.apache.kafka.streams.state.KeyValueIterator;


/**
 * The store used in the followed by operation. It provides the methods {@link FollowedByEventStore#fetchEventsInLeft(Object, long, long, boolean)}
 * {@link FollowedByEventStore#fetchEventsInRight(Object, long, long)} methods, to search for events arrived, respectively,
 * before or after the event whose key is used for the search.
 */
public interface FollowedByEventStore<K, V> extends EventStore<K, V> {

    public KeyValueIterator<K, V> fetchEventsInLeft(K key, long start, long end, boolean delete);

    public KeyValueIterator<K, V> fetchEventsInRight(K key, long start, long end);

}
