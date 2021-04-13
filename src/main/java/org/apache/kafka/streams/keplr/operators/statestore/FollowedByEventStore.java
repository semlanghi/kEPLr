package org.apache.kafka.streams.keplr.operators.statestore;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.WindowStoreIterator;


/**
 * The store used in the followed by operation. It provides the methods {@link FollowedByEventStore#fetchEventsInLeft(Object, long, long, boolean)}
 * {@link FollowedByEventStore#fetchEventsInRight(Object, long, long)} methods, to search for events arrived, respectively,
 * before or after the event whose key is used for the search.
 */
public interface FollowedByEventStore<K, V> extends EventStore<K, V> {

    @Deprecated
    public KeyValueIterator<K, V> fetchEventsInLeft(K key, long start, long end, boolean delete);

    @Deprecated
    public KeyValueIterator<K, V> fetchEventsInRight(K key, long start, long end);

}
