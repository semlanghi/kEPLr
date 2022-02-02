package keplr.operators;

import keplr.etype.EType;
import keplr.etype.TypedKey;
import keplr.operators.statestore.EventOccurrenceEventStore;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;


/**
 * Supplier for the {@link EventOccurrenceProcessor}, which associate events with the same type in one, single,
 * complex event. Gathering the timestamps in a {@link TreeSet} structure in order to keep them ordered.
 * Then, once complete, it uses that structure to perform a {@link EventOccurrenceEventStore#retrieveEvents(Object, long[])}
 * method and {@link ProcessorContext#forward(Object, Object)} the complex event, formed through a
 * {@link org.apache.kafka.streams.kstream.ValueJoiner<K,V,VR>}.
 * @param <K>
 * @param <V>
 * @param <VR>
 */
public class EventOccurrenceSupplier<K,V,VR> extends OrderingAbstractProcessorSupplier<K, V> {

    private final EType<K,V> resultType;
    private final int eventOccurences;
    private final String storeName;
    private static Logger LOGGER = LoggerFactory.getLogger(EventOccurrenceSupplier.class);

    public EventOccurrenceSupplier(EType<K, V> resultType, int eventOccurences,
                                   String storeName, String advancementStoreName) {
        super(resultType, advancementStoreName);
        this.resultType = resultType;
        this.eventOccurences = eventOccurences;
        this.storeName = storeName;
    }

    @Override
    public OrderingAbstractProcessor get() {
        return new EventOccurrenceProcessor();
    }

    private class EventOccurrenceProcessor extends OrderingAbstractProcessor {

        private EventOccurrenceEventStore<TypedKey<K>, V> eventStore;
        private long observedStreamTime = 0;
        private TypedKey<K> activeKey;
        private HashMap<TypedKey<K>, TreeSet<Long>> eventTimestamps = new HashMap<>();

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
                    eventStore.putCompositeEvent(resultType.typed(activeKey.getKey()), array);

                    // FORWARDING

                    forwardCompositeEvent(resultType.typed(activeKey.getKey()), array);

                    if(resultType.isOnEvery())
                        eventTimestamps.get(activeKey).clear();

                }


            }else{
                //Out-of-Order Processing
                LOGGER.debug("Event Occurrence, Out of Order Processing.");
            }
        }

        @Override
        protected void processInternal(TypedKey<K> key, V value) {

        }

        /**
         * Method for the addition of a timestamp in the {@param eventTimestamps} parameter. It takes into account
         * out-of-order computation, i.e., when the collection is full and a late record arrived. More specifically,
         * it adds the new timestamp and removes the new most recent one.
         * @param interval
         * @param time
         */
        private void mergeInternalInterval(TreeSet<Long> interval,long time){
            //Interval Merging, inserting another timestamp

            interval.add(time);

            if(interval.size()>eventOccurences) {
                while (interval.size() > eventOccurences) {
                    interval.remove(interval.first());
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

            TypedKey<K> typedKey = resultType.typed(key.getKey());
            context().forward(typedKey, resultType.wrap(values));
        }
    }


}
