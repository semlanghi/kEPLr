package keplr.operators;

import keplr.etype.EType;
import keplr.etype.TypedKey;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

@Log4j
public abstract class KEPLrAbstractProcessorSupplier<K,V> implements ProcessorSupplier<TypedKey<K>,V> {

    protected final EType<K,V> type;

    public KEPLrAbstractProcessorSupplier(EType<K, V> type) {
        this.type = type;
    }

    @Override
    public abstract KEPLrAbstractProcessor get();

    protected abstract class KEPLrAbstractProcessor extends AbstractProcessor<TypedKey<K>,V> implements KEPLrProcessor<K,V> {


        @Override
        public void init(ProcessorContext context) {
            super.init(context);
        }

        @Override
        public abstract boolean isActive(K key);

        @Override
        public void process(TypedKey<K> key, V value) {
            if (key == null || value == null) {
                log.warn("Null Key or Value. Time: " + context().timestamp());
                return;
            }
            processInternal(key, value);
        }

        protected abstract void processInternal(TypedKey<K> key, V value);
    }
}
