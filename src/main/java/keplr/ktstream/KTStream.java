package keplr.ktstream;

import evaluation.keplr.ApplicationSupplier;
import keplr.etype.TypedKey;
import keplr.operators.statestore.EventOccurrenceStore;
import keplr.operators.statestore.FollowedByStore;
import org.apache.kafka.streams.KeyValue;
import keplr.etype.EType;
import keplr.operators.IntervalEventOccurrenceSupplier;
import keplr.operators.IntervalFollowedBySupplier;
import keplr.operators.OldChunkProcessorSupplier;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The KTStream represents the a typed version of the {@link KStream}.
 * Every implementation presents an {@link EType} parameter, which represents the
 * stream's type, and the type of every event that is inside that stream, i.e.
 * presents a {@link TypedKey} containing the same type.
 * Being a wrapper, the KTStream contains and extend a {@link KStream} instance
 * characterized by typed events (with a {@link TypedKey}).
 *
 * @param <K> The original key class
 * @param <V> The value class
 */
public interface KTStream<K, V> extends KStream<TypedKey<K>, V> {

    /**
     * The every instruction sets up the correlated chunking operation, by setting the
     * stream's type with the {@link EType#isOnEvery()} flag.
     *
     * @see OldChunkProcessorSupplier
     * @return An every-typed version of the current instance.
     */
    public KTStream<K, V> every();

    /**
     * The followed by operation, in order to detect a particular event followed by another.
     * It is considered equal to a join operation between to {@link KTStream} instances.
     * For feasibility reasons, it takes also a parameter to set the scope of the search.
     *
     * @see IntervalFollowedBySupplier
     * @see FollowedByStore
     * @param otherStream The other {@link KTStream}
     * @param withinMs The maximum distance allowed between a predecessor and a successor event.
     * @param joiner The joiner to merge two events.
     * @param <R>
     * @return The merged typed stream, with a composite type
     */
    public <R> KTStream<K, V> followedBy(final KTStream<K, V> otherStream, final long withinMs,
                                         final ValueJoiner<V, V, R> joiner);

    /**
     * The followed by operation, in order to detect a particular event followed by another.
     * It is considered equal to a join operation between to {@link KTStream} instances.
     * For feasibility reasons, it takes also a parameter to set the scope of the search.
     * In this case, the {@link ValueJoiner} from the type of the resulting composite stream,
     * through the {@link EType#joiner()} method.
     *
     * @see IntervalFollowedBySupplier
     * @see FollowedByStore
     * @param otherStream The other {@link KTStream}
     * @param withinMs The maximum distance allowed between a predecessor and a successor event.
     * @return The merged typed stream, with a composite type
     */
    public KTStream<K, V> followedBy(final KTStream<K, V> otherStream, final long withinMs);

    public KStream<TypedKey<K>, V> wrappedStream();

    public EType<K, V> type();

    /**
     * Method to set up the accumulation of events of the same type. It can be seen as a
     * n-join operation. In particular, the {@link EType#product(EType, boolean)} method is applied
     * iteratively to compose a composite type composed by the wrapping of the current type for n times.
     *
     * @see IntervalEventOccurrenceSupplier
     * @see EventOccurrenceStore
     * @param i The number of events we want to accumulate.
     * @return The composite stream formed by composite accumulated events.
     */
    public KTStream<K, V> times(int i);


    KTStream<K, V> merge(final KTStream<K, V> stream);

    /**
     *
     *
     * @see KStream
     * @param stream The original {@link KStream}
     * @param types The {@link EType} instances to match
     * @param <K> The origtypedStreams[0].times(1).every().followedBy(typedStreams[1].times(1).every(), 60000L)
                .to("output_final_part1");inal key class
     * @param <V> The value class
     * @return An array of {@link KTStream}, each one containing a type specified in the input.
     */
    public static <K, V> KTStream<K, V>[] match(KStream<K, V> stream, EType<K, V>... types) {
        Iterator<EType<K, V>> typeIterator = Arrays.asList(types).iterator();

        List<KTStream<K, V>> typedStreams = Arrays.stream(stream.branch(types))
                .map(kvkStream -> {
                    EType<K, V> tempType = typeIterator.next();

                    KTStream<K, V> stream1 = new KTStreamImpl<>(kvkStream.map((key, value) -> new KeyValue<>(tempType.typed(key), value)), tempType, tempType.kClass());
                    return stream1;
                }).collect(Collectors.toList());

        KTStreamImpl<K, V>[] streams = new KTStreamImpl[typedStreams.size()];
        streams = typedStreams.toArray(streams);

        return streams;
    }

    public KTStream<K, V> throughput(ApplicationSupplier app);

    public KTStreamImpl<K,V> chunk();
}
