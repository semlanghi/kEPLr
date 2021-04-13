package org.apache.kafka.streams.keplr.operators;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.keplr.operators.statestore.FollowedByEventStore;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.util.HashMap;
import java.util.Map;

public class FollowedBySupplier<K,V,R> extends OrderingAbstractProcessorSupplier<K,V> {

    private final EType<K,V> predType;
    private final EType<K,V> succType;

    private final String eventStoreName;
    private final String predecessorGatewayStoreName;
    private final String successorGatewayStoreName;
    private final long withinTime;

    private final ValueJoiner<? super V, ? super V, ? extends R> joiner;

    public FollowedBySupplier(final EType<K, V> predType, final EType<K, V> succType, final EType<K, V> resultType,
                              final String advancementStoreName, final String predecessorGatewayStoreName,
                              final String successorGatewayStoreName, final String eventStoreName, long withinTime,
                              final ValueJoiner<? super V, ? super V, ? extends R> joiner) {
        super(resultType, advancementStoreName);
        this.predType = predType;
        this.succType = succType;
        this.eventStoreName = eventStoreName;
        this.withinTime = withinTime;
        this.joiner = joiner;
        this.predecessorGatewayStoreName = predecessorGatewayStoreName;
        this.successorGatewayStoreName = successorGatewayStoreName;
    }

    @Override
    public OrderingAbstractProcessor get() {
        return new FollowedByProcessor();
    }

    private class FollowedByProcessor extends OrderingAbstractProcessor{

        private WindowStore<TypedKey<K>, V> eventStore;
        private KeyValueStore<K, Integer> predecessorGatewayStore;
        private KeyValueStore<K, Integer> successorGatewayStore;

        private String predTypeDescr;
        private String succTypeDescr;

        private Map<K,TypedKey<K>> searchableKey;


        @SuppressWarnings("unchecked")
        @Override
        public void init(final ProcessorContext context) {
            super.init(context);
            this.eventStore = (WindowStore<TypedKey<K>, V>) context.getStateStore(eventStoreName);
            this.predecessorGatewayStore = (KeyValueStore<K, Integer>) context.getStateStore(predecessorGatewayStoreName);
            this.successorGatewayStore = (KeyValueStore<K, Integer>) context.getStateStore(successorGatewayStoreName);
            this.searchableKey = new HashMap<>();
            this.predTypeDescr = predType.getDescription();
            this.succTypeDescr = succType.getDescription();
        }



        @Override
        protected void processInternal(TypedKey<K> key, V value) {

            if(key.getType().equals(predTypeDescr)){
                //It's a predecessor
                searchableKey.putIfAbsent(key.getKey(), key);
                eventStore.put(key,value, context().timestamp());
                if(!predType.isOnEvery()) {
                    predecessorGatewayStore.put(key.getKey(),0);
                }
            }else if (key.getType().equals(succTypeDescr)){
                //It's a successor
                //TODO: cover the case where a successor arrives without any predecessor available
                WindowStoreIterator<V> iterator = eventStore.fetch(searchableKey.get(key.getKey()),
                        context().timestamp() - withinTime, context().timestamp());

                if(!succType.isOnEvery()) {
                    predecessorGatewayStore.put(key.getKey(),1);
                }

                while (iterator.hasNext()){
                    KeyValue<Long,V> keyValue = iterator.next();
                    context().forward(type.typed(key.getKey()), joiner.apply(keyValue.value, value));
                }
            }
        }

    }
}
