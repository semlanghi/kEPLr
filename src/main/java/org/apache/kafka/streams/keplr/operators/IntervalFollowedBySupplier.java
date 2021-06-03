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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static org.apache.kafka.streams.state.RocksDBConfigSetter.LOG;

public class IntervalFollowedBySupplier<K,V,R> implements ProcessorSupplier<TypedKey<K>,V> {

    private final EType<K,V> predType;
    private final EType<K,V> succType;
    private final EType<K,V> resultType;

    private final HashMap<EType<K,V>, Boolean> everysConfig;

    private final String storeName;
    private final long withinTime;
    private static Logger LOGGER = LoggerFactory.getLogger(IntervalFollowedBySupplier.class);

    private final ValueJoiner<? super V, ? super V, ? extends R> joiner;

    public IntervalFollowedBySupplier(final EType<K, V> predType, final EType<K, V> succType,
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
        try {
            return new FollowedByNewProcessor((EType<K, V>) predType.clone(), (EType<K, V>) succType.clone(), (EType<K, V>) resultType.clone(), (HashMap<EType<K, V>, Boolean>) everysConfig.clone());
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private class FollowedByNewProcessor extends AbstractProcessor<TypedKey<K>,V>{

        private FollowedByEventStore<TypedKey<K>, V> eventStore;
        private long streamTime=0;
        private boolean keyset;
        private TypedKey<K> searchKey;
        private int lookBack=1;
        private boolean firstGone=false;
        private long lastPredArrived=0;
        private boolean withinReset;
        private Sensor sensor;
        boolean toReset;

        private EType<K,V> predType;
        private EType<K,V> succType;
        private EType<K,V> resultType;

        private HashMap<EType<K,V>, Boolean> everysConfig;

        public FollowedByNewProcessor(EType<K, V> predType, EType<K, V> succType, EType<K, V> resultType, HashMap<EType<K, V>, Boolean> everysConfig) {
            this.predType = predType;
            this.succType = succType;
            this.resultType = resultType;
            this.everysConfig = everysConfig;
        }

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
                return;
            }

            if(context().timestamp()>=streamTime){
                //NORMAL PROCESSING

                if(!resultType.isChunkRight())
                {
                    if(lastPredArrived+withinTime<=context().timestamp()){
                        toReset=true;
                    }
                }

                if(key.getType().equals(predType.getDescription())){

                    if(toReset){
                        firstGone=false;
                        toReset=false;
                    }

                    if(!firstGone){
                        eventStore.putIntervalEvent(key,value,predType.start(value),context().timestamp(), !predType.isChunkLeft());
                        if(!everysConfig.get(predType)){
                            firstGone=true;
                            lastPredArrived =context().timestamp();
                        }
                    }
                }
                else {
                    //eventStore.putIntervalEvent(key,value,succType.start(value),context().timestamp(), true);
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
                } else{
                    searchKey=predType.typed(key.getKey());

                    iter = eventStore.fetchEventsInLeft(searchKey,succType.start(value), inputRecordTimestamp,  !everysConfig.get(succType));


                    //Forced to do this, since the only case in which we reset the search for the A, in the case A->every B
                    // is when we reach the whithin, so basically have to restart the A

                    if(!resultType.isChunkRight())
                        {
                        if(lastPredArrived+withinTime<context().timestamp()){
                            toReset=true;
                        }
                    }

                    while (iter.hasNext()) {
                        final KeyValue<TypedKey<K>, V> otherRecord = iter.next();
                        context().forward(resultType.typed(key.getKey()), joiner.apply(otherRecord.value, value));
                    }
                }
                streamTime=context().timestamp();
            }else {
                //OUT-OF-ORDER
                LOGGER.info("Out of order followed by.");
            }
        }
    }
}
