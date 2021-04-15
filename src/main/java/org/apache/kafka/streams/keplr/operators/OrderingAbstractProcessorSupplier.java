package org.apache.kafka.streams.keplr.operators;

import lombok.extern.log4j.Log4j;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.state.KeyValueStore;

@Log4j
public abstract class OrderingAbstractProcessorSupplier<K,V> extends KEPLrAbstractProcessorSupplier<K,V> {

    protected final String advancementStoreName;

    public OrderingAbstractProcessorSupplier(EType<K, V> type, String advancementStoreName) {
        super(type);
        this.advancementStoreName = advancementStoreName;
    }

    @Override
    public abstract OrderingAbstractProcessor get();

    protected abstract class OrderingAbstractProcessor extends KEPLrAbstractProcessor {

        protected KeyValueStore<K, Long> advancementStore;

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            this.advancementStore = (KeyValueStore<K, Long>) context.getStateStore(advancementStoreName);
        }


        @Override
        public boolean isActive(K key) {
            advancementStore.putIfAbsent(key, context().timestamp());
            return context().timestamp() >= advancementStore.get(key);
        }

        @Override
        public void process(TypedKey<K> key, V value) {
            if (key == null || value == null) {
                log.warn("Null Key or Value. Time: " + context().timestamp());
                return;
            }

            if (isActive(key.getKey())) {
                advancementStore.put(key.getKey(), context().timestamp());
                processInternal(key, value);
            } else {
                log.warn("Out-Of-Order not supported. \n" +
                        "Record Key: " + key.toString() + "\n" +
                        "Value: " + value.toString() + "\n" +
                        "Time: " + context().timestamp());
            }
        }

        protected abstract void processInternal(TypedKey<K> key, V value) ;
    }
}
