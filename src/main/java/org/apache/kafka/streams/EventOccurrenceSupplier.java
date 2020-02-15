package org.apache.kafka.streams;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.streams.kstream.internals.KStreamPassThrough;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;

import java.util.*;

public class EventOccurrenceSupplier<K,V,VR> implements ProcessorSupplier<TypedKey<K>,V> {

    private final EType<K,V> compositeType;
    private final EType<K,V> singleElementType;
    private final int externalWithin;
    private final int eventOccurences;
    private final String storeName;
    private final boolean every;




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
                    eventStore.putCompositeEvent(compositeType.typed(activeKey.key), array);

                    // FORWARDING

                    forwardCompositeEvent(compositeType.typed(activeKey.getKey()), array);

                    if(compositeType.isOnEvery())
                        eventTimestamps.get(activeKey).clear();

                }


            }else{
                //Out-of-Order Processing
                outOfOrder = true;

                /*
                Here I can take also in account interval events, by fetching with the start and ending time
                 */
                /*KeyValueIterator<TypedKey<K>,long[]> overlappingStates = eventStore.fetchEvents(activeKey, context().timestamp());

                while(overlappingStates.hasNext()){

                    //Merge found intervals with the new event, restoring it, and if full, forward it.
                    KeyValue<TypedKey<K>,long[]> composite = overlappingStates.next();
                    long[] nwArray = mergeInterval(composite.value, context().timestamp());

                    //Update the composite event in the store

                    eventStore.putCompositeEvent(activeKey, nwArray);
                    forwardCompositeEvent(compositeType.typed(activeKey.getKey()), nwArray);

                }

                 */




            }

            //eventTimestamps.get(activeKey).clear();

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

        private long[] mergeInterval(long[] array, long time){

            //Interval Merging, inserting another timestamp
            long[] nwArray = ArrayUtils.add(array, time);
            Arrays.sort(nwArray);
            if(array.length>=eventOccurences)
                return ArrayUtils.subarray(nwArray,0,eventOccurences);
            else return nwArray;

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
