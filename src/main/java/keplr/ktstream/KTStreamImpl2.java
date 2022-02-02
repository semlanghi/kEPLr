package keplr.ktstream;

import evaluation.keplr.ApplicationSupplier;
import keplr.etype.TypedKey;
import keplr.operators.FollowedBySupplier;
import keplr.operators.GatewayProcessorSupplier;
import keplr.operators.ThroughputSupplier;
import keplr.operators.statestore_non_interval.FollowedByStoreBuilderNew;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KeyValue;
import keplr.etype.EType;
import org.apache.kafka.streams.keplr.operators.*;
import keplr.operators.statestore_non_interval.FollowedByBytesStoreSupplierNew;
import keplr.operators.statestore_non_interval.FollowedByEventStoreNew;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.*;
import org.apache.kafka.streams.kstream.internals.graph.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import utils.CustomTypedKeySerde;

import java.util.*;

public class KTStreamImpl2<K,V> extends KStreamImpl<TypedKey<K>, V>  implements KTStream<K, V>{

    private static final String FOLLOWEDBY_NAME = "KSTREAM-FOLLOWEDBY-";
    private static final String FOLLOWEDBY_STORE_NAME = "KSTREAM-FOLLOWEDBY-";
    private static final String GATEWAY_NAME = "KSTREAM-GATEWAY-";
    private static final String GATEWAY_STORE_NAME = "KSTREAM-GATEWAY-";
    private static final String THROUGHPUT_NAME = "KSTREAM-THROUGHPUT-";
    private static final String MERGE_NAME = "KSTREAM-MERGE-";

    private final EType<K,V> type;

    public KTStreamImpl2(String name, Serde<TypedKey<K>> keySerde, Serde<V> valueSerde, Set<String> sourceNodes, boolean repartitionRequired, StreamsGraphNode streamsGraphNode, InternalStreamsBuilder builder, EType<K, V> type) {
        super(name, keySerde, valueSerde, sourceNodes, repartitionRequired, streamsGraphNode, builder);
        this.type = type;
    }

    @Override
    public KTStream<K, V> every() {
        try {
            EType<K,V> anotherType = type.clone();
            anotherType.setOnEvery(true);
            return new KTStreamImpl2<>(name, keySerde, valSerde, sourceNodes, false, streamsGraphNode, builder, anotherType);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        } return null;
    }

    @Override
    public <R> KTStream<K, V> followedBy(KTStream<K, V> otherStream, long withinMs, ValueJoiner<V, V, R> joiner) {

        final String processorName = builder.newProcessorName(FOLLOWEDBY_NAME);
        final String followedByStoreName = builder.newStoreName(FOLLOWEDBY_STORE_NAME);
        final String leftStoreName = builder.newStoreName(GATEWAY_STORE_NAME);
        final String rightStoreName = builder.newStoreName(GATEWAY_STORE_NAME);

        KTStreamImpl2<K,V> left = (KTStreamImpl2<K, V>) gateway(leftStoreName);
        KTStreamImpl2<K,V> right = (KTStreamImpl2<K, V>) gateway(rightStoreName);

        FollowedByBytesStoreSupplierNew storeSupplier = new FollowedByBytesStoreSupplierNew(followedByStoreName, withinMs*2, 100L, false,
                5L, withinMs);
        StoreBuilder<FollowedByEventStoreNew<TypedKey<K>,V>> supportStore = new FollowedByStoreBuilderNew<>(storeSupplier, keySerde, valSerde, Time.SYSTEM);

        builder.addStateStore(supportStore);

        EType<K,V> resultType = this.type.product(otherStream.type(), false);

        final StatefulProcessorNode<TypedKey<K>, V> followedByNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<>(new FollowedBySupplier<>(this.type, otherStream.type(), resultType, "store", leftStoreName, rightStoreName, followedByStoreName, withinMs, resultType.joiner()), processorName),
                new String[]{followedByStoreName,leftStoreName,rightStoreName}
        );

        builder.addGraphNode(Arrays.asList(left.streamsGraphNode, right.streamsGraphNode), followedByNode);

        return new KTStreamImpl2<>(name, keySerde, valSerde, this.sourceNodes, false, followedByNode, builder, resultType);

    }

    @Override
    public KTStream<K, V> followedBy(KTStream<K, V> otherStream, long withinMs) {

        final String processorName = builder.newProcessorName(FOLLOWEDBY_NAME);
        final String followedByStoreName = builder.newStoreName(FOLLOWEDBY_STORE_NAME);
        final String leftStoreName = builder.newStoreName(GATEWAY_STORE_NAME);
        final String rightStoreName = builder.newStoreName(GATEWAY_STORE_NAME);
        final String advancementStoreName = builder.newStoreName(GATEWAY_STORE_NAME);

        KTStreamImpl2<K,V> left = (KTStreamImpl2<K, V>) gateway(leftStoreName);
        KTStreamImpl2<K,V> right = (KTStreamImpl2<K, V>) ((KTStreamImpl2<K, V>) otherStream).gateway(rightStoreName);

        FollowedByBytesStoreSupplierNew storeSupplier =
                new FollowedByBytesStoreSupplierNew(followedByStoreName, withinMs*2,
                        100L, false, 5L, withinMs);
        StoreBuilder<FollowedByEventStoreNew<TypedKey<K>,V>> supportStore =
                new FollowedByStoreBuilderNew<>(storeSupplier, keySerde, valSerde, Time.SYSTEM);

        StoreBuilder<KeyValueStore<K, Long>> advancementStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(advancementStoreName),
                ((CustomTypedKeySerde<K>)keySerde).getOriginalKeySerde(), Serdes.Long()
        );

        builder.addStateStore(supportStore);
        builder.addStateStore(advancementStoreBuilder);

        EType<K,V> resultType = this.type.product(otherStream.type(), false);

        final StatefulProcessorNode<TypedKey<K>, V> followedByNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<>(new FollowedBySupplier<>(this.type, otherStream.type(), resultType, advancementStoreName, leftStoreName, rightStoreName, followedByStoreName, withinMs, resultType.joiner()), processorName),
                new String[]{followedByStoreName,leftStoreName,rightStoreName, advancementStoreName}
        );

        builder.addGraphNode(Arrays.asList(left.streamsGraphNode, right.streamsGraphNode), followedByNode);

        return new KTStreamImpl2<>(name, keySerde, valSerde, this.sourceNodes, false, followedByNode, builder, resultType);
    }

    private KTStream<K,V> gateway(String storeName){
        final String processorName = builder.newProcessorName(GATEWAY_NAME);

        StoreBuilder<KeyValueStore<K, Integer>> storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(storeName),
                ((CustomTypedKeySerde<K>)keySerde).getOriginalKeySerde(), Serdes.Integer()
                );

        final StatefulProcessorNode<TypedKey<K>, V> gatewayNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<>(new GatewayProcessorSupplier<>(this.type, storeName), processorName),
                storeBuilder
        );

        builder.addGraphNode(this.streamsGraphNode, gatewayNode);

        return new KTStreamImpl2<>(name,keySerde, valSerde, this.sourceNodes, false, gatewayNode, builder, this.type);
    }

    @Override
    public KStream<TypedKey<K>, V> wrappedStream() {
        return this;
    }

    @Override
    public EType<K, V> type() {
        return type;
    }

    @Override
    public KTStream<K, V> times(int i) {
        return null;
    }

    @Override
    public KTStream<K, V> throughput(ApplicationSupplier app) {
        final String processorName = builder.newProcessorName(THROUGHPUT_NAME);

        final ProcessorGraphNode<TypedKey<K>, V> throughputNode = new ProcessorGraphNode<>(
                processorName,
                new ProcessorParameters<>(new ThroughputSupplier<>(this.type, app)
                        , processorName)
        );

        builder.addGraphNode(Collections.singletonList(this.streamsGraphNode), throughputNode);

        return new KTStreamImpl2<>(name, keySerde, valSerde, this.sourceNodes, false, throughputNode, builder, this.type);

    }

    @Override
    public KTStreamImpl<K, V> chunk() {
        return null;
    }

    @Override
    public void to(String topic) {
        super.map((KeyValueMapper<TypedKey<K>, V, KeyValue<K, V>>) (key, value) -> new KeyValue<>(key.getKey(), value))
                .to(topic, Produced.with(((CustomTypedKeySerde<K>)keySerde).getOriginalKeySerde(),valueSerde()));
    }

    @Override
    public KTStream<K, V> merge(KTStream<K, V> stream) {
        final KTStreamImpl2<K, V> streamImpl = (KTStreamImpl2<K, V>) stream;
        final String name = builder.newProcessorName(MERGE_NAME);
        final Set<String> allSourceNodes = new HashSet<>();

        allSourceNodes.addAll(sourceNodes);
        allSourceNodes.addAll(streamImpl.sourceNodes);

        final ProcessorParameters<? super K, ? super V> processorParameters = new ProcessorParameters<>(new KStreamPassThrough<>(), name);

        final ProcessorGraphNode<? super K, ? super V> mergeNode = new ProcessorGraphNode<>(name, processorParameters);

        mergeNode.setMergeNode(true);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, streamImpl.streamsGraphNode), mergeNode);

        // drop the serde as we cannot safely use either one to represent both streams
        return new KTStreamImpl2<>(name, keySerde, valSerde, allSourceNodes, false, mergeNode, builder, type.union(streamImpl.type));
    }
}
