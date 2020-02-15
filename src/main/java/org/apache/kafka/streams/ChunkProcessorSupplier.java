package org.apache.kafka.streams;

import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;
import com.brein.time.timeintervals.intervals.NumberInterval;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

public class ChunkProcessorSupplier<K,V> implements ProcessorSupplier<TypedKey<K>,V> {

    private final EType<K,V> type;
    private long mithinMs;
    private long lastStart=0;

    public ChunkProcessorSupplier(EType<K, V> type, long mithinMs) {
        this.type = type;
        this.mithinMs = mithinMs;
    }

    public ChunkProcessorSupplier(EType<K, V> type) {
        this.type = type;
    }

    @Override
    public Processor<TypedKey<K>, V> get() {
        return new ChunkProcessor();
    }

    private class ChunkProcessor extends AbstractProcessor<TypedKey<K>,V> implements Processor<TypedKey<K>,V>{


        private long observedStreamTime = 0;
        private long lastIteration = 0;
        private long focus;
        private NumberInterval<Long> lastValidEvent;
        private NumberInterval<Long> actualSession = new LongInterval(0L,0L);

        private Sensor sensor;


        private boolean outOfOrder = false;


        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            sensor = context.metrics().addLatencyAndThroughputSensor("kEPLr-scope", "kEPLr", "chunk-on-"+type.description, Sensor.RecordingLevel.INFO);
        }

        @Override
        public void process(TypedKey<K> key, V value) {

            sensor.record();

            if(key==null || value==null){
                System.out.println("Chiave null");
                return;

            }

            if(context().timestamp()>=observedStreamTime){
                //Normal Processing

                outOfOrder = false;

                observedStreamTime = context().timestamp();



                //System.out.println(key.getType().toString());
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

                                //System.out.println(lastIteration);
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
