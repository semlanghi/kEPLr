package org.apache.kafka.streams.keplr.ktstream;

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
    //private Set<String> originNode;


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

    /*
    private KTStream(KStream<TypedKey<K>, V> stream, EType<K, V> type, Class<K> kClass, Set<String> originNode){
        super((KStreamImpl<TypedKey<K>,V>)stream);
        this.type = type;
        this.wrappedStream = stream;
        this.kClass = kClass;
        this.originNode = originNode;
    }


    public void setOriginNode(String originNode) {
        this.originNode = new HashSet<>();
        this.originNode.add(originNode);
    }*/

    /*@Override
    public KTStream<K,V> every(){

        final String processorName = builder.newProcessorName("KTSTREAM-EVERY-");
        EType<K,V> everyType = this.type.everyVersion();
        final ProcessorGraphNode<TypedKey<K>, V> everyNode = new ProcessorGraphNode<>(
                processorName,
                new ProcessorParameters<>(new EveryChunkedSupplier<>()
                        , processorName)
        );

        //final ProcessorParameters<TypedKey<K>, V> processorParameters = new ProcessorParameters<TypedKey<K>, V>(
        //final ProcessorGraphNode<TypedKey<K>, V> followedByNode = new ProcessorGraphNode<>(processorName, processorParameters);
        builder.addGraphNode(Collections.singletonList(this.streamsGraphNode), everyNode);

        return new KTStreamImpl<>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, everyNode, builder), everyType, kClass);
    }*/

    @Override
    public KTStream<K,V> every(){

        final String processorName = builder.newProcessorName("KTSTREAM-EVERY-");
        this.type = this.type.everyVersion();
        return this;
    }

    private KTStreamImpl<K,V> chunk(){

        final String processorName = builder.newProcessorName("KTSTREAM-CHUNK-");

        final ProcessorGraphNode<TypedKey<K>, V> chunkNode = new ProcessorGraphNode<>(
                processorName,
                new ProcessorParameters<>(new ChunkProcessorSupplier<>(this.type)
                        , processorName)
        );


        //final ProcessorParameters<TypedKey<K>, V> processorParameters = new ProcessorParameters<TypedKey<K>, V>(
        //final ProcessorGraphNode<TypedKey<K>, V> followedByNode = new ProcessorGraphNode<>(processorName, processorParameters);
        builder.addGraphNode(Collections.singletonList(this.streamsGraphNode), chunkNode);


        return new KTStreamImpl<>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, chunkNode, builder), this.type, kClass);

    }

    private KTStreamImpl<K,V> throughput(){

        final String processorName = builder.newProcessorName("KTSTREAM-THROUGHPUT-");

        final ProcessorGraphNode<TypedKey<K>, V> throughputNode = new ProcessorGraphNode<>(
                processorName,
                new ProcessorParameters<>(new ThroughputSupplier<>(this.type)
                        , processorName)
        );

        builder.addGraphNode(Collections.singletonList(this.streamsGraphNode), throughputNode);

        return new KTStreamImpl<>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, throughputNode, builder), this.type, kClass);

    }

    /*

    public KTStream<K,V> every(){

        StatefulProcessorNode node = (StatefulProcessorNode) this.streamsGraphNode;

        System.out.println("STREAM GRAPH NODE IS "+node.nodeName()+node.processorParameters().processorName());

        String name = builder.newProcessorName("EVERY-");

        final StatefulProcessorNode<TypedKey<K>, V> node1 = new StatefulProcessorNode<TypedKey<K>,V>(
                name,
                new ProcessorParameters<TypedKey<K>, V>(new EverySupplier<K, V>(this.type, node.processorParameters().processorSupplier()),name),
                node.getStoreBuilder()
        );
        this.builder.addGraphNode(this.streamsGraphNode.parentNodes(),node1
                );
        Collection<StreamsGraphNode> coll = this.streamsGraphNode.parentNodes();

        for (StreamsGraphNode temp: coll
        ) {
            if(!originNode.contains(temp.nodeName()))
                wrapNodeEvery((StatefulProcessorNode) temp);
            temp.children().remove(this.streamsGraphNode);
        }

        return new KTStream<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, node1, builder),this.type, kClass, this.originNode);

    }




    private void wrapNodeEvery(StatefulProcessorNode node){
        String name = builder.newProcessorName("EVERY-");

        final StatefulProcessorNode<TypedKey<K>, V> node1 = new StatefulProcessorNode<TypedKey<K>,V>(
                name,
                new ProcessorParameters<TypedKey<K>, V>(new EverySupplier<K, V>(this.type, node.processorParameters().processorSupplier()),name),
                node.getStoreBuilder()
        );


        for (StreamsGraphNode temp: this.streamsGraphNode.children()
        ) {

            node1.addChild(temp);
        }
        this.streamsGraphNode.clearChildren();


        for (StreamsGraphNode temp: this.streamsGraphNode.parentNodes()
        ) {

            temp.children().remove(this.streamsGraphNode);
            temp.addChild(node1);
        }
        this.streamsGraphNode.parentNodes().clear();

        for (StreamsGraphNode temp: this.streamsGraphNode.parentNodes()){
            if(originNode.contains(temp.nodeName()))
                return;
            if(temp instanceof StatefulProcessorNode)
                wrapNodeEvery((StatefulProcessorNode) temp);
        }
    }

    private KTStream<K,V> after(EType<K,V> otherType, String storeName, long withinMs){

        final String processorName = builder.newProcessorName("KTSTREAM-AFTER-");
        final ProcessorGraphNode<TypedKey<K>, V> afterNode = new ProcessorGraphNode<>(
                processorName,
                new ProcessorParameters<>(new AfterSupplier<>(this.type, otherType, storeName, withinMs)
                        , processorName)
        );


        //final ProcessorParameters<TypedKey<K>, V> processorParameters = new ProcessorParameters<TypedKey<K>, V>(
        //final ProcessorGraphNode<TypedKey<K>, V> followedByNode = new ProcessorGraphNode<>(processorName, processorParameters);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode), afterNode);

        return new KTStream<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, afterNode, builder),this.type, kClass, this.originNode);

    }



    public <R> KTStream<K,V> followedByFirst(final KTStream<K, V> otherStream, final long withinMs,
                                       final ValueJoiner<V, V, R> joiner){




        final String processorName = builder.newProcessorName("KTSTREAM-FOLLOWEDBY-");

        UUID id = UUID.randomUUID();

        FollowedByWindowBytesStoreSupplier storeSupplier = new FollowedByWindowBytesStoreSupplier("_Store_"+id, 5L, 100L, false,
        5L, Long.MAX_VALUE);


        final StoreBuilder<EventStore<TypedKey<K>,V>> supportStore = new FollowedByStoreBuilder<>(storeSupplier, new TypedKeySerde<K>(kClass), this.valSerde, Time.SYSTEM);

        EType<K,V> resultType = this.type.product(otherStream.type, false);

        KTStream<K,V> afterStream= otherStream.after(this.type,"_Store_"+id, withinMs);
        KStreamImpl<K,V> othStream = (KStreamImpl<K, V>) afterStream.wrappedStream;
        HashMap<EType<K, V>, Boolean> everysConfig = new HashMap<>();
        everysConfig.put(this.type, true);
        everysConfig.put(otherStream.type, false);

        final StatefulProcessorNode<TypedKey<K>, V> followedByNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<>(new FollowedByFirstSupplier<>(this.type,otherStream.type, resultType, everysConfig, "_Store_"+id, withinMs, joiner),processorName),
                supportStore
        );


        //final ProcessorParameters<TypedKey<K>, V> processorParameters = new ProcessorParameters<TypedKey<K>, V>(
        //final ProcessorGraphNode<TypedKey<K>, V> followedByNode = new ProcessorGraphNode<>(processorName, processorParameters);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, othStream.streamsGraphNode), followedByNode);

        this.originNode.addAll(otherStream.originNode);

        return new KTStream<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass, this.originNode);
    }


    public <R> KTStream<K,V> followedBy(final KTStream<K, V> otherStream, final long withinMs,
                                                final ValueJoiner<V, V, R> joiner){
        KTStream<K,V> ktStream = this.chunk();
        return ktStream.followedByInternal(otherStream,withinMs,joiner);
    }



    public <R> KTStream<K,V> followedByInternal(final KTStream<K, V> otherStream, final long withinMs,
                                        final ValueJoiner<V, V, R> joiner){



        final String processorName = builder.newProcessorName("KTSTREAM-FOLLOWEDBY-");

        EventStore<Bytes,byte[]> bytesEventStore;
        bytesEventStore = new FollowedByWindowStore("_Store_"+id++, "metrics", 5L, 100L, false, 5,5, withinMs);

        UUID id = UUID.randomUUID();

        FollowedByWindowBytesStoreSupplier storeSupplier = new FollowedByWindowBytesStoreSupplier("_Store_"+id, 5L, 100L, false,
                5L, withinMs);

        final StoreBuilder<EventStore<TypedKey<K>,V>> supportStore = new FollowedByStoreBuilder<>(storeSupplier, new TypedKeySerde<K>(kClass), this.valSerde, Time.SYSTEM);

        KTStream<K,V> othStream = otherStream; //.after(this.type,"_Store_"+id,withinMs );
        KStreamImpl<K,V> kStream = (KStreamImpl<K, V>) othStream.wrappedStream;

        EType<K,V> resultType = this.type.product(othStream.type, false);


        HashMap<EType<K, V>, Boolean> everysConfig = new HashMap<>();
        everysConfig.put(this.type, this.type.isOnEvery());
        everysConfig.put(othStream.type, othStream.type.isOnEvery());

        resultType.chunk(!everysConfig.get(this.type),!everysConfig.get(othStream.type));

        final StatefulProcessorNode<TypedKey<K>, V> followedByNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<>(new FollowedBySupplier<>(this.type,othStream.type, resultType, everysConfig, "_Store_"+id, withinMs, joiner),processorName),
                supportStore
        );


        //final ProcessorParameters<TypedKey<K>, V> processorParameters = new ProcessorParameters<TypedKey<K>, V>(
        //final ProcessorGraphNode<TypedKey<K>, V> followedByNode = new ProcessorGraphNode<>(processorName, processorParameters);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, othStream.streamsGraphNode), followedByNode);

        this.originNode.addAll(othStream.originNode);

        return new KTStream<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass, this.originNode);
    }*/

    @Override
    public <R> KTStream<K,V> followedBy(final KTStream<K, V> otherStream, final long withinMs,
                                        final ValueJoiner<V, V, R> joiner){
        //KTStream<K,V> ktStreamImpl = this.chunk();
        return this.chunk().followedByInternal(otherStream,withinMs,joiner);
    }

    @Override
    public KTStream<K,V> followedBy(final KTStream<K, V> otherStream, final long withinMs
                                           ){
        //KTStream<K,V> ktStreamImpl =
        return this.chunk().followedByInternal(otherStream,withinMs);
    }

    @Override
    public KStream<TypedKey<K>, V> wrappedStream() {
        return wrappedStream;
    }


    public KTStream<K,V> followedByInternal(final KTStream<K, V> otherStream, final long withinMs){

        final String processorName = builder.newProcessorName("KTSTREAM-FOLLOWEDBY-");

        //EventStore<Bytes,byte[]> bytesEventStore = new FollowedByWindowStore("_Store_"+id++, "metrics", 5L, 100L, false, 5,5, withinMs);

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


        //final ProcessorParameters<TypedKey<K>, V> processorParameters = new ProcessorParameters<TypedKey<K>, V>(
        //final ProcessorGraphNode<TypedKey<K>, V> followedByNode = new ProcessorGraphNode<>(processorName, processorParameters);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, kStream.streamsGraphNode), followedByNode);

        //this.originNode.addAll(othStream.originNode);

        return new KTStreamImpl<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass);
    }


/*
    public KTStream<K,V> followedByInternal(final KTStream<K, V> otherStream, final long withinMs){

        final String processorName = builder.newProcessorName("KTSTREAM-FOLLOWEDBY-");

        //EventStore<Bytes,byte[]> bytesEventStore = new FollowedByWindowStore("_Store_"+id++, "metrics", 5L, 100L, false, 5,5, withinMs);

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
                new ProcessorParameters<>(new FollowedBySupplierWOChunk<>(this.type, otherStream.type(), resultType, everysConfig, "_Store_"+id, withinMs, resultType.joiner()),processorName),
                supportStore
        );


        //final ProcessorParameters<TypedKey<K>, V> processorParameters = new ProcessorParameters<TypedKey<K>, V>(
        //final ProcessorGraphNode<TypedKey<K>, V> followedByNode = new ProcessorGraphNode<>(processorName, processorParameters);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, kStream.streamsGraphNode), followedByNode);

        //this.originNode.addAll(othStream.originNode);

        return new KTStreamImpl<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass);
    }


 */

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


        //final ProcessorParameters<TypedKey<K>, V> processorParameters = new ProcessorParameters<TypedKey<K>, V>(
        //final ProcessorGraphNode<TypedKey<K>, V> followedByNode = new ProcessorGraphNode<>(processorName, processorParameters);
        builder.addGraphNode(Arrays.asList(this.streamsGraphNode, kStream.streamsGraphNode), followedByNode);

        //this.originNode.addAll(othStream.originNode);

        return new KTStreamImpl<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass);
    }

    public KTStream<K,V> times(final int eventOccurrences){

        final String processorName = builder.newProcessorName("KTSTREAM-TIMES-");

        //EventStore<Bytes,byte[]> bytesEventStore;

        //bytesEventStore = new FollowedByWindowStore("_Store_"+id++, "metrics", 5L, 100L, false, 5,5, Long.MAX_VALUE);

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

    /*
    public <R> KTStream<K,V> timesFirst(final int eventOccurrences){

        final String processorName = builder.newProcessorName("KTSTREAM-TIMES-");

        EventStore<Bytes,byte[]> bytesEventStore;

        bytesEventStore = new FollowedByWindowStore("_Store_"+id++, "metrics", 5L, 100L, false, 5,5, Long.MAX_VALUE);

        UUID id = UUID.randomUUID();

        FollowedByWindowBytesStoreSupplier storeSupplier = new FollowedByWindowBytesStoreSupplier("_Store_"+id, 100L, 100L, false,
                100L, Long.MAX_VALUE);

        final StoreBuilder<EventStore<TypedKey<K>,V>> supportStore = new FollowedByStoreBuilder<>(storeSupplier, new TypedKeySerde<K>(kClass), this.valSerde, Time.SYSTEM);

        EType<K,V> resultType;
        if(eventOccurrences==1)
            resultType = type;
        else resultType = type.product(type, true);


        for(int i=1; i<eventOccurrences; i++){
            resultType=resultType.product(type, true);
        }

        final StatefulProcessorNode<TypedKey<K>, V> followedByNode = new StatefulProcessorNode<TypedKey<K>, V>(
                processorName,
                new ProcessorParameters<TypedKey<K>, V>(new EventOccurrenceFirstSupplier<K, V>(resultType,this.type,Integer.MAX_VALUE, eventOccurrences,  "_Store_"+id,false),processorName),
                supportStore
        );

        builder.addGraphNode(Arrays.asList(this.streamsGraphNode), followedByNode);

        return new KTStream<K,V>(new KStreamImpl<>(name, null, null, this.sourceNodes, false, followedByNode, builder),resultType, kClass, originNode);
    }



    public static <K,V> KTStreamImpl<K, V>[] match(KStream<K,V> stream, EType<K,V>... types){

        Iterator<EType<K,V>> typeIterator = Arrays.asList(types).iterator();

        List<KTStreamImpl<K, V>> typedStreams = Arrays.stream(stream.branch(types))
                .map(kvkStream -> {
                    EType<K,V> tempType = typeIterator.next();

                    KTStreamImpl<K,V> stream1 = new KTStreamImpl<>(kvkStream.map((key, value) -> new KeyValue<>(tempType.typed(key), value)), tempType, tempType.kClass());
                    //stream1.setOriginNode(stream1.streamsGraphNode.nodeName());
                    return stream1;
                }).collect(Collectors.toList());

        KTStreamImpl<K, V>[] streams = new KTStreamImpl[typedStreams.size()];
        streams=typedStreams.toArray(streams);

        return streams;

    }



    public KStream followedByJoin(final KTStream<K,V> otherStream, final long withinMs,
                                  final ValueJoiner<V, V, V> joiner){


        KStreamImplJoinNew join = new KStreamImplJoinNew(false, false);


        //return  join.join(wrappedStream, otherStream.wrappedStream, joiner,
             //   JoinWindows.of(Duration.ofMillis(0)).after(Duration.ofMillis(withinMs)),
               // Joined.with(null, null,null));

        return null;
    };*/

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

            //protected -> public
            final StoreBuilder<WindowStore<K, V>> thisWindowStore =
                    joinWindowStoreBuilder(joinThisName, windows, joined.keySerde(), joined.valueSerde());
            final StoreBuilder<WindowStore<K, V>> otherWindowStore =
                    joinWindowStoreBuilder(joinOtherName, windows, joined.keySerde(), joined.otherValueSerde());

            //private class -> public
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




    /*@Override
    public KTStream<K,V>[] branch(Predicate<? super TypedKey<K>, ? super V>... predicates) {


        List<KTStream<K,V>> streams = Arrays.stream(wrappedStream.branch(predicates)).map(new Function<KStream<K,V>, KTStream<K,V>>() {
            @Override
            public KTStream<K, V> apply(KStream<K, V> kStream) {
                return new KTStream<>(kStream,type);
            }

        }).collect(Collectors.toList());

        KTStream<K,V>[] array = new KTStream[streams.size()];
        array = streams.toArray(array);





        return null;


    }

     */

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
        KTStreamImpl<K,V> stream = this.chunk().throughput();

        stream.wrappedStream.map(new KeyValueMapper<TypedKey<K>, V, KeyValue<K, V>>() {
            @Override
            public KeyValue<K, V> apply(TypedKey<K> key, V value) {
                    return new KeyValue<>(key.getKey(),value);
            }
        }).to(topic);
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
