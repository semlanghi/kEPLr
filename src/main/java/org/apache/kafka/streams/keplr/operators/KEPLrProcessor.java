package org.apache.kafka.streams.keplr.operators;

import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.processor.Processor;

public interface KEPLrProcessor<K,V> extends Processor<TypedKey<K>,V> {
    boolean isActive(K key);
}
