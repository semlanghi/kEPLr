package keplr.operators;

import keplr.etype.EType;
import keplr.etype.TypedKey;
import lombok.extern.log4j.Log4j;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

@Log4j
public class GatewayProcessorSupplier<K,V> extends KEPLrAbstractProcessorSupplier<K,V> {

    private final String gateStoreName;

    public GatewayProcessorSupplier(EType<K, V> type, String gateStoreName) {
        super(type);
        this.gateStoreName = gateStoreName;
    }

    @Override
    public KEPLrAbstractProcessor get() {
        return new GatewayProcessor();
    }

    private class GatewayProcessor extends KEPLrAbstractProcessor {

        private KeyValueStore<K,Integer> gateStore;

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            this.gateStore = (KeyValueStore<K, Integer>) context.getStateStore(gateStoreName);
        }

        @Override
        public boolean isActive(K key) {
            Integer isOpen = gateStore.get(key);
            if(isOpen != null)
                return isOpen.equals(1);
            else {
                gateStore.put(key,1);
                return true;
            }
        }

        @Override
        protected void processInternal(TypedKey<K> key, V value) {
            if(isActive(key.getKey()))
                context().forward(key,value);
        }
    }
}
