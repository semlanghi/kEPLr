package org.apache.kafka.streams.keplr.operators;

import com.brein.time.timeintervals.intervals.LongInterval;
import com.brein.time.timeintervals.intervals.NumberInterval;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class ChunkProcessorSupplier<K,V> implements ProcessorSupplier<TypedKey<K>,V> {

    private final EType<K,V> type;

    public ChunkProcessorSupplier(EType<K, V> type) {
        this.type = type;
    }

    @Override
    public Processor<TypedKey<K>, V> get() {
        try {
            return new ChunkProcessor((EType<K,V>) type.clone());
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        return null;
    }

    private class ChunkProcessor extends AbstractProcessor<TypedKey<K>,V> implements Processor<TypedKey<K>,V>{

        private EType<K,V> type;
        private long observedStreamTime = 0;
        private long lastIteration = 0;
        private long focus;
        private long counter;
        private long lastStart=0;


        private boolean outOfOrder = false;

        public ChunkProcessor(EType<K,V> type) {
            this.type = type;
        }

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

                outOfOrder = false;

                observedStreamTime = context().timestamp();
                counter++;
                //System.out.println(counter + " partition "+context().partition() + " real partition " + key.getKey().toString() + " task "+context().taskId()+" Thread "+Thread.currentThread().getName());

                if(type.isOnEvery()) {
                    //Chunking
                    if (type.isChunkRight())
                        focus = type.start(value);
                    else {
                        focus = type.end(value);
                    }
                    if (focus > lastIteration) {
                        if(!type.isChunkRight() && type.isChunkLeft()){
                            if(type.start(value)==lastStart){
                                lastIteration=type.end(value);

                                context().forward(key,value);
                                return;
                            }else{
                                if(type.start(value)>lastIteration){
                                    lastIteration=type.end(value);
                                    lastStart=type.start(value);
                                    context().forward(key,value);
                                    return;
                                }
                            }
                        }else{
                            context().forward(key,value);
                            if (!type.isChunkRight() && !type.isChunkLeft()) {
                                lastIteration = -1;
                            } else {
                                if (type.isChunkLeft())
                                    lastIteration = type.end(value);
                                else lastIteration = type.start(value);
                            }
                        }
                    }
                }else{
                    context().forward(key, value);
                }

            }else{
                //Out-of-Order Processing
                outOfOrder=true;
                System.out.println("Every chunk, out of order.");
            }
        }
    }
}
