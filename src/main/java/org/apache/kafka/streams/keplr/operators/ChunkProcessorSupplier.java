package org.apache.kafka.streams.keplr.operators;

import com.brein.time.timeintervals.intervals.IInterval;
import com.brein.time.timeintervals.intervals.LongInterval;
import com.brein.time.timeintervals.intervals.NumberInterval;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkProcessorSupplier<K,V> implements ProcessorSupplier<TypedKey<K>,V> {

    private final EType<K,V> type;
    private static Logger LOGGER = LoggerFactory.getLogger(ChunkProcessorSupplier.class);


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
        private long lastStart=0;
        private Sensor sensor;

        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            sensor = context.metrics().addLatencyAndThroughputSensor("kEPLr-scope", "kEPLr", "chunk-on-"+type.getDescription(), Sensor.RecordingLevel.INFO);
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
                observedStreamTime = context().timestamp();

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
                LOGGER.debug("Every chunk, out of order. Time of the event" + context().timestamp()+" stream time "+observedStreamTime+" Key "+key);
            }
        }
    }
}
