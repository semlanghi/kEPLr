package org.apache.kafka.streams;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.HashMap;

import static org.apache.kafka.streams.state.RocksDBConfigSetter.LOG;

public class FollowedBySupplierWOChunk <K,V,R> implements ProcessorSupplier<TypedKey<K>,V> {

    private final EType<K,V> predType;
    private final EType<K,V> succType;
    private final EType<K,V> resultType;

    private final HashMap<EType<K,V>, Boolean> everysConfig;

    private final String storeName;
    private final long withinTime;

    private final ValueJoiner<? super V, ? super V, ? extends R> joiner;

    public FollowedBySupplierWOChunk(final EType<K, V> predType, final EType<K, V> succType,
                                 EType<K, V> resultType, HashMap<EType<K, V>, Boolean> everysConfig, final String storeName, long withinTime, final ValueJoiner<? super V, ? super V, ? extends R> joiner) {
        this.predType = predType;
        this.succType = succType;
        this.resultType = resultType;
        this.everysConfig = everysConfig;
        this.storeName = storeName;
        this.withinTime = withinTime;
        this.joiner = joiner;
        if(!this.everysConfig.get(predType) && !this.everysConfig.get(succType)){
            this.everysConfig.put(predType,true);
            this.everysConfig.put(succType,true);

        }

    }

    @Override
    public Processor<TypedKey<K>, V> get() {
        return new FollowedByWOChunkProcessor();
    }

    private class FollowedByWOChunkProcessor extends AbstractProcessor<TypedKey<K>,V> {

        private FollowedByEventStore<TypedKey<K>, V> eventStore;
        private long streamTime=0;
        private TypedKey<K> searchKey;
        private boolean firstGone=false;
        private long lastValidPred =0;
        private long lastReset=0;

        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            eventStore = (FollowedByEventStore<TypedKey<K>, V>) context.getStateStore(storeName );
        }

        @Override
        public void process(final TypedKey<K> key, final V value) {

            if (key == null || value == null || key.getKey() == null) {
                streamTime=context().timestamp();
                LOG.warn(
                        "Skipping record due to null key or value. key=[{}] value=[{}] topic=[{}] partition=[{}] offset=[{}]",
                        key, value, context().topic(), context().partition(), context().offset()
                );
                //metrics.skippedRecordsSensor().record();

                return;
            }

            if(context().timestamp()>=streamTime){
                //NORMAL PROCESSING


                long inputRecordTimestamp = context().timestamp();

                KeyValueIterator<TypedKey<K>,V> iter;
                if(key.getType().equals(predType.description)){
                    //PREDECESSOR


                    // se e' on every, salvo tutti i predecessori
                    if(predType.isOnEvery()) {
                        eventStore.putIntervalEvent(key, value, predType.start(value), context().timestamp(), false);
                        lastValidPred =context().timestamp();
                    } else {
                        //altrimenti salvo solo primo
                        if (!firstGone) {
                            eventStore.putIntervalEvent(key, value, predType.start(value), context().timestamp(), !predType.isChunkLeft());
                            lastValidPred =context().timestamp();
                        }
                    }

                    searchKey=succType.typed(key.getKey());
                    iter = eventStore.fetchEventsInRight(searchKey, inputRecordTimestamp, inputRecordTimestamp+withinTime);


                }else{
                    //SUCCESSOR

                    if(inputRecordTimestamp<lastValidPred+withinTime){
                        searchKey=predType.typed(key.getKey());
                        iter = eventStore.fetchEventsInLeft(searchKey,succType.start(value), inputRecordTimestamp,  !everysConfig.get(succType));

                        while (iter.hasNext()) {
                            final KeyValue<TypedKey<K>, V> otherRecord = iter.next();
                            context().forward(
                                    resultType.typed(key.getKey()),
                                    joiner.apply(otherRecord.value, value));
                        }
                    }else{

                    }



                }

                streamTime=context().timestamp();
            }else {
                //OUT-OF-ORDER

                System.out.println("Out of order followed by.\n");
            }





            //}


        }

        private void resetIteration(){
            lastReset = context().timestamp();



        }


    }
}
