package org.apache.kafka.streams;

import org.apache.kafka.streams.state.KeyValueIterator;

public interface FollowedByEventStore<K,V> extends EventStore<K,V> {

    public KeyValueIterator<K,V> fetchEventsInLeft(K key, long start, long end, boolean delete);

    public KeyValueIterator<K,V> fetchEventsInRight(K key, long start, long end);

}
