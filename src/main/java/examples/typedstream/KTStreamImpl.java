package examples.typedstream;

import examples.typedstream.EType;
import examples.typedstream.KTStream;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.*;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorGraphNode;
import org.apache.kafka.streams.kstream.internals.graph.ProcessorParameters;
import org.apache.kafka.streams.kstream.internals.graph.StreamStreamJoinNode;
import org.apache.kafka.streams.kstream.internals.graph.StreamsGraphNode;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.WindowStore;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class KTStreamImpl<K,V,T> implements KTStream<K,V,T> {

    EType<V,T> type;
    KStream<K,V> stream;


    public KTStreamImpl() {
    }

    @Override
    public KTStream match(KStream stream, EType type) {
        this.type = type;
        this.stream = stream;
        return this;
    }

    @Override
    public <VO, VR> KStream<K, VR> followedBy(KTStream<K, VO, T> otherStream, ValueJoiner<? super V, ? super VO, ? extends VR> joiner, JoinWindows windows) {


//        if(value.getSchema().getName()==predecessor)
//            store.put(value, actual);
        /*
        final String thisWindowStreamName = builder.newProcessorName(WINDOWED_NAME);
        final String otherWindowStreamName = builder.newProcessorName(WINDOWED_NAME);
        final String joinThisName = rightOuter ? builder.newProcessorName(OUTERTHIS_NAME) : builder.newProcessorName(JOINTHIS_NAME);
        final String joinOtherName = leftOuter ? builder.newProcessorName(OUTEROTHER_NAME) : builder.newProcessorName(JOINOTHER_NAME);
        final String joinMergeName = builder.newProcessorName(MERGE_NAME);*/

        /*final StreamsGraphNode thisStreamsGraphNode = ((AbstractStream) lhs).streamsGraphNode;
        final StreamsGraphNode otherStreamsGraphNode = ((AbstractStream) other).streamsGraphNode;


        final StoreBuilder<WindowStore<K1, V1>> thisWindowStore =
                joinWindowStoreBuilder(joinThisName, windows, joined.keySerde(), joined.valueSerde());
        final StoreBuilder<WindowStore<K1, V2>> otherWindowStore =
                joinWindowStoreBuilder(joinOtherName, windows, joined.keySerde(), joined.otherValueSerde());

        final KStreamJoinWindow<K1, V1> thisWindowedStream = new KStreamJoinWindow<>(thisWindowStore.name());

        final ProcessorParameters<K1, V1> thisWindowStreamProcessorParams = new ProcessorParameters<>(thisWindowedStream, thisWindowStreamName);
        final ProcessorGraphNode<K1, V1> thisWindowedStreamsNode = new ProcessorGraphNode<>(thisWindowStreamName, thisWindowStreamProcessorParams);
        builder.addGraphNode(thisStreamsGraphNode, thisWindowedStreamsNode);

        final KStreamJoinWindow<K1, V2> otherWindowedStream = new KStreamJoinWindow<>(otherWindowStore.name());

        final ProcessorParameters<K1, V2> otherWindowStreamProcessorParams = new ProcessorParameters<>(otherWindowedStream, otherWindowStreamName);
        final ProcessorGraphNode<K1, V2> otherWindowedStreamsNode = new ProcessorGraphNode<>(otherWindowStreamName, otherWindowStreamProcessorParams);
        builder.addGraphNode(otherStreamsGraphNode, otherWindowedStreamsNode);

        final KStreamKStreamJoin<K1, R, V1, V2> joinThis = new KStreamKStreamJoin<>(
                otherWindowStore.name(),
                windows.beforeMs,
                windows.afterMs,
                joiner,
                leftOuter
        );

        final KStreamKStreamJoin<K1, R, V2, V1> joinOther = new KStreamKStreamJoin<>(
                thisWindowStore.name(),
                windows.afterMs,
                windows.beforeMs,
                reverseJoiner(joiner),
                rightOuter
        );

        final KStreamPassThrough<K1, R> joinMerge = new KStreamPassThrough<>();

        final StreamStreamJoinNode.StreamStreamJoinNodeBuilder<K1, V1, V2, R> joinBuilder = StreamStreamJoinNode.streamStreamJoinNodeBuilder();

        final ProcessorParameters<K1, V1> joinThisProcessorParams = new ProcessorParameters<>(joinThis, joinThisName);
        final ProcessorParameters<K1, V2> joinOtherProcessorParams = new ProcessorParameters<>(joinOther, joinOtherName);
        final ProcessorParameters<K1, R> joinMergeProcessorParams = new ProcessorParameters<>(joinMerge, joinMergeName);

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

        final Set<String> allSourceNodes = new HashSet<>(((KStreamImpl<K1, V1>) lhs).sourceNodes);
        allSourceNodes.addAll(((KStreamImpl<K1, V2>) other).sourceNodes);

        // do not have serde for joined result;
        // also for key serde we do not inherit from either since we cannot tell if these two serdes are different
        return new KStreamImpl<>(joinMergeName, joined.keySerde(), null, allSourceNodes, false, joinGraphNode, builder);
*/ return null;
    }



    @Override
    public KStream filter(Predicate predicate) {
        return null;
    }

    @Override
    public KStream filterNot(Predicate predicate) {
        return null;
    }

    @Override
    public void print(Printed printed) {

    }

    @Override
    public void foreach(ForeachAction action) {

    }

    @Override
    public KStream peek(ForeachAction action) {
        return null;
    }

    @Override
    public KStream[] branch(Predicate[] predicates) {
        return new KStream[0];
    }

    @Override
    public KStream merge(KStream stream) {
        return null;
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
        stream.to(topic);
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

    }

    @Override
    public KGroupedStream groupByKey() {
        return null;
    }

    @Override
    public KGroupedStream groupByKey(Serialized serialized) {
        return null;
    }

    @Override
    public KGroupedStream groupByKey(Grouped grouped) {
        return null;
    }

    @Override
    public KStream leftJoin(GlobalKTable globalKTable, KeyValueMapper keyValueMapper, ValueJoiner valueJoiner) {
        return null;
    }

    @Override
    public KStream join(GlobalKTable globalKTable, KeyValueMapper keyValueMapper, ValueJoiner joiner) {
        return null;
    }

    @Override
    public KStream leftJoin(KTable table, ValueJoiner joiner, Joined joined) {
        return null;
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
