package org.apache.kafka.streams.keplr.operators;

import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.operators.statestore.FollowedByEventStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.HashMap;

import static org.apache.kafka.streams.state.RocksDBConfigSetter.LOG;

public class FollowedBySupplierNew<K,V,R> implements ProcessorSupplier<TypedKey<K>,V> {

    private final EType<K,V> predType;
    private final EType<K,V> succType;
    private final EType<K,V> resultType;

    private final HashMap<EType<K,V>, Boolean> everysConfig;

    private final String storeName;
    private final long withinTime;

    private final ValueJoiner<? super V, ? super V, ? extends R> joiner;

    public FollowedBySupplierNew(final EType<K, V> predType, final EType<K, V> succType,
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
        return new FollowedByNewProcessor();
    }

    private class FollowedByNewProcessor extends AbstractProcessor<TypedKey<K>,V>{

        private FollowedByEventStore<TypedKey<K>, V> eventStore;
        private long streamTime=0;
        private boolean keyset;
        private TypedKey<K> searchKey;
        private int lookBack=1;
        private boolean firstGone=false;
        private long lastPredArrived=Long.MIN_VALUE;
        private boolean withinReset;
        private Sensor sensor;


        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            eventStore = (FollowedByEventStore<TypedKey<K>, V>) context.getStateStore(storeName );
           // sensor = context.metrics().addLatencyAndThroughputSensor("scope","entity",
             //       "operation_followed_by", org.apache.kafka.common.metrics.Sensor.RecordingLevel.INFO);
        }

        //TODO: classe intermedia, AbstractTypedProcessor

        @Override
        public void process(final TypedKey<K> key, final V value) {

            //sensor.record();



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



                if(key.getType().equals(predType.getDescription())){


                    if(!firstGone){
                        eventStore.putIntervalEvent(key,value,predType.start(value),context().timestamp(), !predType.isChunkLeft());

                        if(!everysConfig.get(predType)){
                            firstGone=true;
                            lastPredArrived =context().timestamp();
                        }

                    }
                }
                else {
                    eventStore.putIntervalEvent(key,value,succType.start(value),context().timestamp(), true);
                }

                long inputRecordTimestamp = context().timestamp();

                /*
                If the type is the predecessor, search for events that FOLLOWS the event,
                else search for events the are FOLLOWED BY the event arrived.
                 */
                KeyValueIterator<TypedKey<K>,V> iter = null;
                if(key.getType().equals(predType.getDescription())){
                    searchKey=succType.typed(key.getKey());
                    iter = eventStore.fetchEventsInRight(searchKey, inputRecordTimestamp, inputRecordTimestamp+withinTime);
                }else{
                    searchKey=predType.typed(key.getKey());
                    //System.out.println(everysConfig.get(succType));
                   /* if(resultType.isChunkLeft())
                        iter = eventStore.fetchEventsInLeft(searchKey, Math.max(inputRecordTimestamp,lastPredArrived), succType.start(value), !everysConfig.get(succType));
                    else */

                   iter = eventStore.fetchEventsInLeft(searchKey,succType.start(value), inputRecordTimestamp,  !everysConfig.get(succType));
//
//                    if(iter.hasNext() && resultType.isChunkRight()){
//                        //withinReset = true;
//                        firstGone=false;
//                    }

                    //Forced to do this, since the only case in which we reset the search for the A, in the case A->every B
                    // is when we reach the whithin, so basically have to restart the A

                        if(!resultType.isChunkRight())
                            {
                            if(lastPredArrived+withinTime<context().timestamp()){
                                firstGone=false;
                            }
                        }




                    while (iter.hasNext()) {
                        final KeyValue<TypedKey<K>, V> otherRecord = iter.next();
                        context().forward(
                                resultType.typed(key.getKey()),
                                joiner.apply(otherRecord.value, value));
                    }






                }

                streamTime=context().timestamp();
            }else {
                //OUT-OF-ORDER

                System.out.println("Out of order followed by.\n");
            }





            //}


        }


    }
}
