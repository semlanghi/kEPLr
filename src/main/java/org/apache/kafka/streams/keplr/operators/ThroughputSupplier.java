package org.apache.kafka.streams.keplr.operators;

import com.opencsv.CSVWriter;
import evaluation.ExperimentsConfig;
import evaluation.keplr.WBase;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;


public class ThroughputSupplier<K,V> implements ProcessorSupplier<TypedKey<K>,V> {

    EType<K,V> type;

    public ThroughputSupplier(EType<K, V> type) {
        this.type = type;
    }

    @Override
    public Processor<TypedKey<K>, V> get() {
        return new ThroughputProcessor();
    }

    private class ThroughputProcessor extends AbstractProcessor<TypedKey<K>,V> implements Processor<TypedKey<K>,V>{

        private long observedStreamTime = 0;
        CSVWriter writer;
        ProcessorContext context;
        Properties config = (Properties) WBase.config.clone();
        long startProc = System.currentTimeMillis();
        long counter = 0;


        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            this.context = context;

            try {
                this.writer = new CSVWriter(
                        new FileWriter(config.getProperty(ExperimentsConfig.EXPERIMENT_OUTPUT), true));
            } catch (IOException e) {
                e.printStackTrace();
            }


        }

        @Override
        public void process(TypedKey<K> key, V value) {

            if(key==null || value==null){
                System.out.println("Chiave null");
                return;
            }

            if(context().timestamp()>=observedStreamTime){
                //Normal Processing

                counter++;

                observedStreamTime = context().timestamp();

                if(type.isThisTheEnd(value)){
                    Runtime runtime = Runtime.getRuntime();
                    long memoryUsed = runtime.totalMemory() - runtime.freeMemory();
                    String thread = Thread.currentThread().getName();
                    Integer partition = Integer.parseInt((String) key.getKey());
                    Object a_count = ((GenericRecord)((GenericRecord)value).get("x")).get("idA");
                    Object b_count = ((GenericRecord)((GenericRecord)value).get("y")).get("idB");
                    writer.writeNext(new String[]{
                            config.getProperty(ExperimentsConfig.EXPERIMENT_NAME),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_RUN),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_BROKER_COUNT),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_INIT_CHUNK_SIZE),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_NUM_CHUNKS),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_CHUNK_GROWTH),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_WINDOW),
                            String.valueOf(startProc), String.valueOf(System.currentTimeMillis()),
                            String.valueOf(counter),
                            String.valueOf(a_count), String.valueOf(b_count), String.valueOf(partition), thread,
                            String.valueOf(memoryUsed)
                    }, false);

                    try {
                        writer.flush();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    context().forward(key, value);
//                    try {
//                        Thread.sleep(60000);
//                        //runtime.exit(0);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }


                }else {
                    context().forward(key, value);
                }


            }else{
                //Out-of-Order Processing
                System.out.println("Throughput Processor, out of order.");
            }
        }
    }
}
