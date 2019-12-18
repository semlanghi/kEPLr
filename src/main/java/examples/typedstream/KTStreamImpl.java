package examples.typedstream;

import examples.typedstream.EType;
import examples.typedstream.KTStream;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TopicNameExtractor;

public class KTStreamImpl<K,V,T> implements KTStream<K,V,T> {

    EType<V,T> type;

    public KTStreamImpl() {
    }

    public KTStreamImpl(EType<V,T> type) {
        this.type = type;
    }

    @Override
    public KTStream match(KStream stream, EType type) {
        return new KTStreamImpl(type);
    }

    @Override
    public KStream followedBy(KTStream otherStream, ValueJoiner joiner, JoinWindows windows) {



        return null;
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
