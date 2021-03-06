package org.apache.kafka.streams.keplr.operators;

import com.opencsv.CSVWriter;
import evaluation.ExperimentsConfig;
import evaluation.keplr.ApplicationSupplier;
import evaluation.keplr.WBase;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Supplier for the {@link ThroughputProcessor}, which stops the computation once the END event arrived, providing
 * the different metrics on a CSV file. The ending of the computation is performed through the {@link ApplicationSupplier}, which
 * collects the ending of all tasks, i.e., {@link ApplicationSupplier#close()}, and then perform the actual stop.
 *
 * @see ApplicationSupplier
 * @see CSVWriter
 * @param <K>
 * @param <V>
 */
public class ThroughputSupplier<K, V> implements ProcessorSupplier<TypedKey<K>, V> {

    private final ApplicationSupplier app;
    EType<K, V> type;
    private static Logger LOGGER = LoggerFactory.getLogger(ThroughputSupplier.class);


    public ThroughputSupplier(EType<K, V> type, ApplicationSupplier app) {
        this.type = type;
        this.app = app;
    }

    @Override
    public Processor<TypedKey<K>, V> get() {
        return new ThroughputProcessor();
    }

    private class ThroughputProcessor extends AbstractProcessor<TypedKey<K>, V> implements Processor<TypedKey<K>, V> {

        private long observedStreamTime = 0;
        CSVWriter throughput;
        private CSVWriter memory;
        ProcessorContext context;
        Properties config = (Properties) WBase.config.clone();
        long startProc = System.currentTimeMillis();
        long counter = 0;
        List<Integer> partitions = new ArrayList<>();
        private long experiment_window;
        private long current_window = 0L;
        private String run;
        private String name;


        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            this.context = context;
            String thread = Thread.currentThread().getName();

            try {
                this.experiment_window = Long.parseLong(config.getProperty(ExperimentsConfig.EXPERIMENT_WINDOW));

                this.run = config.getProperty(ExperimentsConfig.EXPERIMENT_RUN);
                this.name = config.getProperty(ExperimentsConfig.EXPERIMENT_NAME);
                this.throughput = new CSVWriter(new FileWriter(config.getProperty(ExperimentsConfig.EXPERIMENT_OUTPUT), true));
                this.memory = new CSVWriter(new FileWriter(name + "." + run + ".memory.csv", true));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void process(TypedKey<K> key, V value) {

            if (key == null || value == null) {
                System.out.println("Chiave null");
                return;
            }

            String thread = Thread.currentThread().getName();
            if (context().timestamp() >= observedStreamTime) {
                Runtime runtime = Runtime.getRuntime();
                //Normal Processing
                counter++;
                observedStreamTime = context().timestamp();

                GenericRecord value1 = (GenericRecord) value;
                long endtime = (Long) value1.get("end_time");
                if (endtime > current_window) {
                    long memoryUsed = runtime.totalMemory() - runtime.freeMemory();
                    memory.writeNext(new String[]{
                            name,
                            run,
                            String.valueOf(counter),
                            String.valueOf(memoryUsed),
                            String.valueOf(System.currentTimeMillis()),
                            thread,
                            String.valueOf(current_window),
                            String.valueOf(endtime),
                            String.valueOf(observedStreamTime)
                    }, false);
                    current_window += experiment_window;
                    try {
                        memory.flush();
                    } catch (IOException e) {

                    }
                }

                if (type.isThisTheEnd(value)) {
                    long memoryUsed = runtime.totalMemory() - runtime.freeMemory();

                    String key2 = (String) key.getKey();
                    Object a_count = ((GenericRecord) ((GenericRecord) value).get("x")).get("idA");
                    Object a_partition = ((GenericRecord) ((GenericRecord) value).get("x")).get("partition");
                    Object b_count = ((GenericRecord) ((GenericRecord) value).get("y")).get("idB");
                    Object b_partition = ((GenericRecord) ((GenericRecord) value).get("y")).get("partition");
                    throughput.writeNext(new String[]{
                            name,
                            run,
                            config.getProperty(ExperimentsConfig.EXPERIMENT_BROKER_COUNT),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_INIT_CHUNK_SIZE),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_NUM_CHUNKS),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_CHUNK_GROWTH),
                            config.getProperty(ExperimentsConfig.EXPERIMENT_WINDOW),
                            String.valueOf(startProc), String.valueOf(System.currentTimeMillis()),
                            String.valueOf(counter),
                            String.valueOf(a_count), String.valueOf(b_count), String.valueOf(key2), String.valueOf(a_partition), String.valueOf(b_partition), thread,
                            String.valueOf(memoryUsed)
                    }, false);

                    try {
                        throughput.flush();

                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    context().forward(key, value);
                    synchronized (app) {
                        app.close();
                    }
                } else {

                    context().forward(key, value);
                }


            } else {
                //Out-of-Order Processing
                LOGGER.debug("Throughput Processor, out of order.");
            }
        }
    }
}
