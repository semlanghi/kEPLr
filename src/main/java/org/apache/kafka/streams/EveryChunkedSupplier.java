package org.apache.kafka.streams;

import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class EveryChunkedSupplier<K,V> implements ProcessorSupplier<TypedKey<K>,V> {

    //private final EType<K,V> type;

    @Override
    public Processor<TypedKey<K>, V> get() {
        return new EveryChunkedProcessor();
    }

    private class EveryChunkedProcessor extends AbstractProcessor<TypedKey<K>,V> implements Processor<TypedKey<K>,V>{

        //private EventStore<TypedKey<K>, V> eventStore;
        private long observedStreamTime = 0;
        //private long lastIteration = 0;

        //private boolean outOfOrder = false;


        @Override
        public void init(ProcessorContext context) {
            super.init(context);
        }

        @Override
        public void process(TypedKey<K> key, V value) {

            if(key==null || value==null){
                System.out.println("Chiave null");
                return;
            }

            if(context().timestamp()>=observedStreamTime){
                //Normal Processing

                //outOfOrder = false;
                observedStreamTime = context().timestamp();
                context().forward(key, value);

            }else{
                //Out-of-Order Processing
                //outOfOrder=true;
                //System.out.println("Every chunk, out of order.");
            }
        }
    }

}
