package org.apache.kafka.streams.keplr.ktstream;

import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public interface KTStream<K,V> extends KStream<TypedKey<K>,V> {

    public KTStream<K,V> every();

    public <R> KTStream<K,V> followedBy(final KTStream<K, V> otherStream, final long withinMs,
                                            final ValueJoiner<V, V, R> joiner);
    public KTStream<K,V> followedBy(final KTStream<K, V> otherStream, final long withinMs);

    public KStream<TypedKey<K>,V> wrappedStream();

    public EType<K,V> type();

    public KTStream<K,V> times(int i);

    public static <K, V> KTStream<K, V>[] match(KStream<K, V> stream, EType<K, V>... types) {
        Iterator<EType<K,V>> typeIterator = Arrays.asList(types).iterator();

        List<KTStream<K, V>> typedStreams = Arrays.stream(stream.branch(types))
                .map(kvkStream -> {
                    EType<K,V> tempType = typeIterator.next();

                    KTStream<K,V> stream1 = new KTStreamImpl<>(kvkStream.map((key, value)
                            -> new KeyValue<>(tempType.typed(key), value)), tempType, tempType.kClass());
                    return stream1;
                }).collect(Collectors.toList());

        KTStreamImpl<K, V>[] streams = new KTStreamImpl[typedStreams.size()];
        streams=typedStreams.toArray(streams);

        return streams;
    }
}
