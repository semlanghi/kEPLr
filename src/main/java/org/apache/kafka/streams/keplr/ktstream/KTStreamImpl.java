package org.apache.kafka.streams.keplr.ktstream;

import evaluation.keplr.ApplicationSupplier;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.keplr.operators.ChunkProcessorSupplier;
import org.apache.kafka.streams.keplr.operators.EventOccurrenceSupplier;
import org.apache.kafka.streams.keplr.operators.FollowedBySupplierNew;
import org.apache.kafka.streams.keplr.operators.ThroughputSupplier;
import org.apache.kafka.streams.keplr.operators.statestore.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.*;
import org.apache.kafka.streams.kstream.internals.graph.*;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;
import utils.TypedKeySerde;


import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.kstream.internals.KStreamImpl.joinWindowStoreBuilder;

public class KTStreamImpl<K,V> extends AbstractStream<TypedKey<K>, V>  implements KTStream<K, V>{

    EType<K,V> type;
    KStream<TypedKey<K>, V> wrappedStream;
    final Class<K> kClass;


    private static long id = 0;

    protected KTStreamImpl(KStream<TypedKey<K>, V> stream, EType<K, V> type, Class<K> kClass){
        super((KStreamImpl<TypedKey<K>,V>)stream);
        this.type = type;
        this.wrappedStream = stream;
        this.kClass = kClass;
    }

    public EType<K, V> type() {
        return type;
    }

    @Override
    public KTStream<K,V> every(){

        final String processorName = builder.newProcessorName("KTSTREAM-EVERY-");
        this.type = this.type.everyVersion();
        return this;
    }

    public KTStreamImpl<K,V> chunk(){

        final String processorName = builder.newProcessorName("KTSTREAM-CHUNK-");

        final ProcessorGraphNode<TypedKey<K>, V> chunkNode = new ProcessorGraphNode<>(
                processorName,
                new ProcessorParameters<>(new ChunkProcessorSupplier<>(this.type)
                        , processorName)
        );

        builder.addGraphNode(Collections.singletonList(this.streamsGraphNode), chunkNode);

        return new KTStreamImpl<>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, chunkNode, builder), this.type, kClass);

    }

    public KTStreamImpl<K,V> throughput(ApplicationSupplier app){

        final String processorName = builder.newProcessorName("KTSTREAM-THROUGHPUT-");

        final ProcessorGraphNode<TypedKey<K>, V> throughputNode = new ProcessorGraphNode<>(
                processorName,
                new ProcessorParameters<>(new ThroughputSupplier<>(this.type, app)
                        , processorName)
        );

        builder.addGraphNode(Collections.singletonList(this.streamsGraphNode), throughputNode);

        return new KTStreamImpl<>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, throughputNode, builder), this.type, kClass);

    }

    @Override
    public <R> KTStream<K,V> followedBy(final KTStream<K, V> otherStream, final long withinMs,
                                        final ValueJoiner<V, V, R> joiner){
        return this.chunk().followedByInternal(otherStream,withinMs,joiner);
    }

    @Override
    public KTStream<K,V> followedBy(final KTStream<K, V> otherStream, final long withinMs){
        return this.chunk().followedByInternal(otherStream,withinMs);
    }

    @Override
    public KStream<TypedKey<K>, V> wrappedStream() {
        return wrappedStream;
    }


    public KTStream<K,V> followedByInternal(final KTStream<K, V> otherStream, final long withinMs){

        final String processorName = builder.newProcessorName("KTSTREAM-FOLLOWEDBY-");

        UUID id = UUID.randomUUID();

        FollowedByBytesStoreSupplier storeSupplier = new FollowedByBytesStoreSupplier("_Store_"+id, withinMs*2, 100L, false,
                5L, withinMs);

        final StoreBuilder<FollowedByEventStore<TypedKey<K>,V>> supportStore = new FollowedByStoreBuilder<>(storeSupplier, new TypedKeySerde<K>(kClass), this.valSerde, Time.SYSTEM);

        KStreamImpl<K,V> kStream = (KStreamImpl<K, V>) otherStream.wrappedStream();

        EType<K,V> resultType = this.type.product(otherStream.type(), false);


        HashMap<EType<K, V>, Boolean> everysConfig = new HashMap<>();
        everysConfig.put(this.type, this.type.isOnEvery());
        everysConfig.put(otherStream.type(), otherStream.type().isOnEvery());

        resultType.chunk(!everysConfig.get(this.type),!everysConfig.get(otherStream.type()));

        final StatefulProcessorNode<TypedKey<K>, V> followedByNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<>(new FollowedBySupplierNew<>(this.type, otherStream.type(), resultType, everysConfig, "_Store_"+id, withinMs, resultType.joiner()),processorName),
                supportStore
        );


        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, kStream.streamsGraphNode), followedByNode);

        return new KTStreamImpl<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass);
    }

    public <R> KTStream<K,V> followedByInternal(final KTStream<K, V> otherStream, final long withinMs,
                                                    final ValueJoiner<V, V, R> joiner){



        final String processorName = builder.newProcessorName("KTSTREAM-FOLLOWEDBY-");

        UUID id = UUID.randomUUID();

        FollowedByBytesStoreSupplier storeSupplier = new FollowedByBytesStoreSupplier("_Store_"+id, 100L, 100L, false,
                5L, withinMs);

        final StoreBuilder<FollowedByEventStore<TypedKey<K>,V>> supportStore = new FollowedByStoreBuilder<>(storeSupplier, new TypedKeySerde<K>(kClass), this.valSerde, Time.SYSTEM);

        KStreamImpl<K,V> kStream = (KStreamImpl<K, V>) otherStream.wrappedStream();

        EType<K,V> resultType = this.type.product(otherStream.type(), false);


        HashMap<EType<K, V>, Boolean> everysConfig = new HashMap<>();
        everysConfig.put(this.type, this.type.isOnEvery());
        everysConfig.put(otherStream.type(), otherStream.type().isOnEvery());

        resultType.chunk(!everysConfig.get(this.type),!everysConfig.get(otherStream.type()));

        final StatefulProcessorNode<TypedKey<K>, V> followedByNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<>(new FollowedBySupplierNew<>(this.type, otherStream.type(), resultType, everysConfig, "_Store_"+id, withinMs, joiner),processorName),
                supportStore
        );

        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, kStream.streamsGraphNode), followedByNode);

        return new KTStreamImpl<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass);
    }

    public KTStream<K,V> times(final int eventOccurrences){

        final String processorName = builder.newProcessorName("KTSTREAM-TIMES-");

        UUID id = UUID.randomUUID();

        EventOccurrenceBytesStoreSupplier storeSupplier = new EventOccurrenceBytesStoreSupplier("_Store_"+id, 100L, 100L, false,
                100L, Long.MAX_VALUE);

        final StoreBuilder<EventOccurrenceEventStore<TypedKey<K>,V>> supportStore = new EventOccurrenceStoreBuilder<>(storeSupplier, new TypedKeySerde<K>(kClass), this.valSerde, Time.SYSTEM);

        EType<K,V> resultType;
        if(eventOccurrences==1)
            resultType = type;
        else  resultType = type.product(type, true);


        for(int i=1; i<eventOccurrences; i++){
            resultType=resultType.product(type, true);
        }

        final StatefulProcessorNode<TypedKey<K>, V> followedByNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<TypedKey<K>, V>(new EventOccurrenceSupplier<K, V, V>(resultType,this.type,Integer.MAX_VALUE, eventOccurrences,  "_Store_"+id,false),processorName),
                supportStore
        );

        builder.addGraphNode(Arrays.asList(this.streamsGraphNode), followedByNode);

        return new KTStreamImpl<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass);
    }

    @Override
    public KStream filter(Predicate predicate) {
        return wrappedStream.filter(predicate);
    }


    @Override
    public KStream filterNot(Predicate predicate) {
        return wrappedStream.filterNot(predicate);
    }

    @Override
    public void print(Printed printed) {
        wrappedStream.print(printed);
    }

    private class KStreamImplJoinNew {

        private final boolean leftOuter;
        private final boolean rightOuter;


        KStreamImplJoinNew(final boolean leftOuter,
                           final boolean rightOuter) {
            this.leftOuter = leftOuter;
            this.rightOuter = rightOuter;
        }

        public KStream join(final KStream<K, V> lhs,
                                               final KStream<K, V> other,
                                               final ValueJoiner<? super V, ? super V, ? extends V> joiner,
                                               final JoinWindows windows,
                                               final Joined<K, V, V> joined) {
            final String thisWindowStreamName = builder.newProcessorName("Window_pred_store_"+this);
            final String otherWindowStreamName = builder.newProcessorName("Window_succ_");
            final String joinThisName = builder.newProcessorName("join_proc_1_");
            final String joinOtherName = builder.newProcessorName("join_proc_2_");
            final String joinMergeName = builder.newProcessorName("merge_proc_");

            final StreamsGraphNode thisStreamsGraphNode = ((AbstractStream) lhs).streamsGraphNode;
            final StreamsGraphNode otherStreamsGraphNode = ((AbstractStream) other).streamsGraphNode;

            // Library change: protected -> public
            final StoreBuilder<WindowStore<K, V>> thisWindowStore =
                    joinWindowStoreBuilder(joinThisName, windows, joined.keySerde(), joined.valueSerde());
            final StoreBuilder<WindowStore<K, V>> otherWindowStore =
                    joinWindowStoreBuilder(joinOtherName, windows, joined.keySerde(), joined.otherValueSerde());

            // Library change: private class -> public
            final KStreamJoinWindow<K, V> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.name());

            final ProcessorParameters<K, V> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamName);
            final ProcessorGraphNode<K, V> thisWindowedStreamsNode = new ProcessorGraphNode<>(thisWindowStreamName, thisWindowStreamProcessorParams);
            builder.addGraphNode(thisStreamsGraphNode, thisWindowedStreamsNode);

            final KStreamJoinWindow<K, V> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name());

            final ProcessorParameters<K, V> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamName);
            final ProcessorGraphNode<K, V> otherWindowedStreamsNode = new ProcessorGraphNode<>(otherWindowStreamName, otherWindowStreamProcessorParams);
            builder.addGraphNode(otherStreamsGraphNode, otherWindowedStreamsNode);

            final KStreamKStreamJoin<K, V, V, V> joinThis = new KStreamKStreamJoin<>(
                    otherWindowStore.name(),
                    windows.beforeMs,
                    windows.afterMs,
                    joiner,
                    leftOuter
            );

            final KStreamKStreamJoin<K, V, V, V> joinOther = new KStreamKStreamJoin<>(
                    thisWindowStore.name(),
                    windows.afterMs,
                    windows.beforeMs,
                    reverseJoiner(joiner),
                    rightOuter
            );

            final KStreamPassThrough<K, V> joinMerge = new KStreamPassThrough<>();

            final StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K, V, V, V> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

            final ProcessorParameters<K, V> joinThisProcessorParams = new ProcessorParameters<>(joinThis, joinThisName);
            final ProcessorParameters<K, V> joinOtherProcessorParams = new ProcessorParameters<>(joinOther, joinOtherName);
            final ProcessorParameters<K, V> joinMergeProcessorParams = new ProcessorParameters<>(joinMerge, joinMergeName);

            joinBuilder.withJoinMergeProcessorParameters(joinMergeProcessorParams)
                    .withJoinThisProcessorParameters(joinThisProcessorParams)
                    .withJoinOtherProcessorParameters(joinOtherProcessorParams)
                    .withThisWindowStoreBuilder(thisWindowStore)
                    .withOtherWindowStoreBuilder(otherWindowStore)
                    .withThisWindowedStreamProcessorParameters(thisWindowStreamProcessorParams)
                    .withOtherWindowedStreamProcessorParameters(otherWindowStreamProcessorParams)
                    .withValueJoiner(joiner)
                    .withNodeName(joinMergeName);

            final StreamsGraphNode joinGraphNode = joinBuilder.build();

            builder.addGraphNode(Arrays.asList(thisStreamsGraphNode, otherStreamsGraphNode), joinGraphNode);

            final Set<String> allSourceNodes = new HashSet<>(((KStreamImpl<K, V>) lhs).sourceNodes);
            allSourceNodes.addAll(((KStreamImpl<K, V>) other).sourceNodes);

            // do not have serde for joined result;
            // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
            return new KStreamImpl<>(joinMergeName, joined.keySerde(), null, allSourceNodes, false, joinGraphNode, builder);
        }
    }

    @Override
    public void foreach(ForeachAction action) {
        wrappedStream.foreach(action);
    }

    @Override
    public KStream peek(ForeachAction action) {
        return wrappedStream.peek(action);
    }

    @Override
    public KStream merge(KStream stream) {
        return null;
    }


    @Override
    public KStream<TypedKey<K>, V>[] branch(Predicate<? super TypedKey<K>, ? super V>... predicates) {
        List<KTStreamImpl<K,V>> streams = Arrays.stream(wrappedStream.branch(predicates)).map(new Function<KStream<TypedKey<K>,V>, KTStreamImpl<K,V>>() {
            @Override
            public KTStreamImpl<K, V> apply(KStream<TypedKey<K>, V> typedKeyVKStream) {
                return new KTStreamImpl<K, V>(typedKeyVKStream,type, kClass);
            }

        }).collect(Collectors.toList());

        KTStreamImpl<K,V>[] array = new KTStreamImpl[streams.size()];
        array = streams.toArray(array);

        return array;
    }

    @Override
    public KStream through(String topic) {
        return null;
    }

    @Override
    public KStream through(String topic, Produced produced) {
        return null;
    }

    @Override
    public void to(String topic) {
        KTStreamImpl<K,V> stream = this;
        stream.wrappedStream.map((KeyValueMapper<TypedKey<K>, V, KeyValue<K, V>>) (key, value) -> new KeyValue<>(key.getKey(),value)).to(topic);
    }

    @Override
    public void to(String topic, Produced produced) {

    }

    @Override
    public void to(TopicNameExtractor topicExtractor) {

    }

    @Override
    public void to(TopicNameExtractor topicExtractor, Produced produced) {

    }

    @Override
    public void process(ProcessorSupplier processorSupplier, String... stateStoreNames) {
        wrappedStream.process(processorSupplier, stateStoreNames);
    }

    @Override
    public KGroupedStream groupByKey() {
        return wrappedStream.groupByKey();
    }

    @Override
    public KGroupedStream groupByKey(Serialized serialized) {
        return wrappedStream.groupByKey(serialized);
    }

    @Override
    public KGroupedStream groupByKey(Grouped grouped) {
        return wrappedStream.groupByKey(grouped);
    }

    @Override
    public KStream leftJoin(GlobalKTable globalKTable, KeyValueMapper keyValueMapper, ValueJoiner valueJoiner) {
        return wrappedStream.leftJoin(globalKTable, keyValueMapper, valueJoiner);
    }

    @Override
    public KStream join(GlobalKTable globalKTable, KeyValueMapper keyValueMapper, ValueJoiner joiner) {
        return wrappedStream.join(globalKTable, keyValueMapper, joiner);
    }

    @Override
    public KStream leftJoin(KTable table, ValueJoiner joiner, Joined joined) {
        return wrappedStream.leftJoin(table, joiner, joined);

    }

    @Override
    public KStream leftJoin(KTable table, ValueJoiner joiner) {
        return null;
    }

    @Override
    public KStream join(KTable table, ValueJoiner joiner, Joined joined) {
        return null;
    }

    @Override
    public KStream join(KTable table, ValueJoiner joiner) {
        return null;
    }

    @Override
    public KStream outerJoin(KStream otherStream, ValueJoiner joiner, JoinWindows windows, Joined joined) {
        return null;
    }

    @Override
    public KStream outerJoin(KStream otherStream, ValueJoiner joiner, JoinWindows windows) {
        return null;
    }

    @Override
    public KStream leftJoin(KStream otherStream, ValueJoiner joiner, JoinWindows windows, Joined joined) {
        return null;
    }

    @Override
    public KStream leftJoin(KStream otherStream, ValueJoiner joiner, JoinWindows windows) {
        return null;
    }

    @Override
    public KStream join(KStream otherStream, ValueJoiner joiner, JoinWindows windows, Joined joined) {

        return null;
    }

    @Override
    public KStream join(KStream otherStream, ValueJoiner joiner, JoinWindows windows) {
        return null;
    }

    @Override
    public KGroupedStream groupBy(KeyValueMapper selector, Grouped grouped) {
        return null;
    }

    @Override
    public KGroupedStream groupBy(KeyValueMapper selector, Serialized serialized) {
        return null;
    }

    @Override
    public KGroupedStream groupBy(KeyValueMapper selector) {
        return null;
    }

    @Override
    public KStream flatTransformValues(ValueTransformerWithKeySupplier valueTransformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public KStream flatTransformValues(ValueTransformerSupplier valueTransformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public KStream transformValues(ValueTransformerWithKeySupplier valueTransformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public KStream transformValues(ValueTransformerSupplier valueTransformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public KStream flatTransform(TransformerSupplier transformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public KStream transform(TransformerSupplier transformerSupplier, String... stateStoreNames) {
        return null;
    }

    @Override
    public KStream flatMapValues(ValueMapperWithKey mapper) {
        return null;
    }

    @Override
    public KStream flatMapValues(ValueMapper mapper) {
        return null;
    }

    @Override
    public KStream flatMap(KeyValueMapper mapper) {
        return null;
    }

    @Override
    public KStream mapValues(ValueMapperWithKey mapper) {
        return null;
    }

    @Override
    public KStream mapValues(ValueMapper mapper) {
        return null;
    }

    @Override
    public KStream map(KeyValueMapper mapper) {
        return null;
    }

    @Override
    public KStream selectKey(KeyValueMapper mapper) {
        return null;
    }


}
