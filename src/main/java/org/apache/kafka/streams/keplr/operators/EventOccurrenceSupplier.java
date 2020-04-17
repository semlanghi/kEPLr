package org.apache.kafka.streams.keplr.operators;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.operators.statestore.EventOccurrenceEventStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.kstream.internals.KStreamPassThrough;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class EventOccurrenceSupplier<K,V,VR> implements ProcessorSupplier<TypedKey<K>,V> {

    private final EType<K,V> compositeType;
    private final EType<K,V> singleElementType;
    private final int externalWithin;
    private final int eventOccurences;
    private final String storeName;
    private final boolean every;
    private static Logger LOGGER = LoggerFactory.getLogger(EventOccurrenceSupplier.class);





    public EventOccurrenceSupplier(EType<K, V> compositeType, EType<K, V> singleElementType, int externalWithin, int eventOccurences,
                                   String storeName, boolean every) {
        this.compositeType = compositeType;
        this.singleElementType = singleElementType;
        this.externalWithin = externalWithin;
        this.eventOccurences = eventOccurences;
        this.storeName = storeName;
        this.every = every;
    }

    @Override
    public Processor<TypedKey<K>, V> get() {
        if(eventOccurences==1){
            return new KStreamPassThrough<TypedKey<K>,V>().get();
        }else return new EventOccurrenceProcessor();
    }

    private class EventOccurrenceProcessor extends AbstractProcessor<TypedKey<K>,V> {

        private EventOccurrenceEventStore<TypedKey<K>, V> eventStore;
        private long lastStartingTime = 0;
        private long observedStreamTime = 0;
        private TypedKey<K> activeKey;
        private HashMap<TypedKey<K>, TreeSet<Long>> eventTimestamps = new HashMap<>();
        private int actualIndex;
        private boolean first=true;
        private boolean outOfOrder = false;



        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            eventStore = (EventOccurrenceEventStore<TypedKey<K>, V>) context.getStateStore(storeName);
        }

        @Override
        public void process(TypedKey<K> key, V value) {



            if(key==null || value==null){
                System.out.println("Chiave null");
                return;

            }

            activeKey = key;

            //Check if there is an entry for the actual key
            if(!eventTimestamps.containsKey(activeKey)) {
                eventTimestamps.put(activeKey, new TreeSet<>());
            }

            if(context().timestamp()>=observedStreamTime){
                //Normal Processing, only this considered for now

                outOfOrder = false;

                observedStreamTime = context().timestamp();

                eventStore.put(key,value,context().timestamp());

                mergeInternalInterval(eventTimestamps.get(activeKey),context().timestamp());

                if (eventTimestamps.get(activeKey).size() == eventOccurences) {
                    /*
                    Putting composite event, under form of array, in the store.
                    This way the processing take into account just the time of arrival,
                    using the former to retrieve the value on the state store.
                     */
                    long[] array = eventTimestamps.getOrDefault(activeKey, new TreeSet<>()).stream().mapToLong(Long::longValue).toArray();
                    eventStore.putCompositeEvent(compositeType.typed(activeKey.getKey()), array);

                    // FORWARDING

                    forwardCompositeEvent(compositeType.typed(activeKey.getKey()), array);

                    if(compositeType.isOnEvery())
                        eventTimestamps.get(activeKey).clear();

                }


            }else{
                //Out-of-Order Processing
                outOfOrder = true;
                LOGGER.debug("Event Occurrence, Out of Order Processing.");
            }
        }

        private void mergeInternalInterval(TreeSet<Long> interval,long time){
            //Interval Merging, inserting another timestamp

            interval.add(time);

            if(interval.size()>eventOccurences) {
                while (interval.size() > eventOccurences) {
                    if (!outOfOrder) {
                        interval.remove(interval.first());
                    }


                }
            }


        }

        private void forwardCompositeEvent(TypedKey<K> key, long[] array){
            KeyValueIterator<TypedKey<K>,V> iterator = eventStore.retrieveEvents(activeKey, array);

            ArrayList<V> values = new ArrayList<>();
            while(iterator.hasNext()){
                KeyValue<TypedKey<K>,V> pair = iterator.next();
                values.add(pair.value);
            }

            TypedKey<K> typedKey = compositeType.typed(key.getKey());
            context().forward(typedKey, compositeType.wrap(values));
        }
    }


}
