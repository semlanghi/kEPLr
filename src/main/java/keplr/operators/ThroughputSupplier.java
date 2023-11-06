package keplr.operators;

import com.opencsv.CSVWriter;
import evaluation.ExperimentsConfig;
import evaluation.keplr.ApplicationSupplier;
import keplr.etype.EType;
import keplr.etype.TypedKey;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Supplier for the {@link ThroughputProcessor}, which stops the computation once the END event arrived, providing
 * the different metrics on a CSV file. The ending of the computation is performed through the {@link ApplicationSupplier}, which
 * collects the ending of all tasks, i.e., {@link ApplicationSupplier#close()}, and then perform the actual stop.
 *
 * N.B. This class can be used only with {@link GenericRecord} values, since we need to extracts attributes from events.
 *
 * @see ApplicationSupplier
 * @see CSVWriter
 * @param <K>
 * @param <V>
 */
public class ThroughputSupplier<K, V> extends KEPLrAbstractProcessorSupplier<K, V> {

    private final ApplicationSupplier app;
    private static final Logger LOGGER = LoggerFactory.getLogger(ThroughputSupplier.class);


    public ThroughputSupplier(EType<K, V> type, ApplicationSupplier app) {
        super(type);
        this.app = app;
    }

    @Override
    public KEPLrAbstractProcessor get() {
        return new ThroughputProcessor();
    }

    private class ThroughputProcessor extends KEPLrAbstractProcessor {

        private long observedStreamTime = 0;
        CSVWriter throughput;
        private CSVWriter memory;
        long counter = 0;
        long startProc;
        private long experiment_window;
        private long current_window = 0L;
        private String run;
        private String name;
        private int partitions;


        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            try {
                this.startProc = System.currentTimeMillis();
                this.experiment_window = Long.parseLong(String.valueOf(context.appConfigs().get(ExperimentsConfig.EXPERIMENT_WINDOW)));
                this.run = String.valueOf(context.appConfigs().get(ExperimentsConfig.EXPERIMENT_RUN));
                this.name = String.valueOf(context.appConfigs().get(ExperimentsConfig.EXPERIMENT_NAME));
                this.partitions = Integer.parseInt(String.valueOf(context.appConfigs().get(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT)));
                File fileName = new File(name + "_" + run + "_throughput.csv");
                if (!fileName.exists()) {
                    this.throughput = new CSVWriter(new FileWriter(name + "_" + run + "_throughput.csv", true));
                    String[] headersThroughput = {"exp-name", "run", "thread", "partitions-count", "within", "start-time", "end-time", "nevents", "context-time", "key"};
                    throughput.writeNext(headersThroughput, false);
                    throughput.flush();
                    throughput.close();
                }
                String thread = Thread.currentThread().getName();
                this.memory = new CSVWriter(new FileWriter(name + "_" + run + "_" + thread + "_memory.csv", true));
                String[] headersMemory = {"exp-name", "run", "partitions-count","within", "total-memory", "free-memory",  "sys-time", "lastevent-time" , "context-time","nevents", "key"};
                memory.writeNext(headersMemory, false);
                memory.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void processInternal(TypedKey<K> key, V value) {
            String thread = Thread.currentThread().getName();
            if (context().timestamp() >= observedStreamTime) {
                Runtime runtime = Runtime.getRuntime();
                //Normal Processing
                counter++;
                observedStreamTime = context().timestamp();

                GenericRecord avroValue = (GenericRecord) value;
                long endtime = (Long) avroValue.get("end_time");
                if (endtime > current_window) {
                    writeProgressiveMemory(key, runtime, endtime);
                    current_window += experiment_window;
                }

                if (type.isThisTheEnd(value)) {
                    try {
                        writeThroughput(key, context().timestamp(), thread, runtime);
    //                    context().forward(key, value);

                        memory.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    synchronized (app) {
                        app.close();
                    }

                } else {
//                    context().forward(key, value);
                }
            } else {
                //Out-of-Order Processing
                LOGGER.debug("Throughput Processor, out of order.");
            }
        }

        private void writeThroughput(TypedKey<K> key, long contextTime, String thread, Runtime runtime) throws IOException {
            Integer originalKey = (Integer) key.getKey();
            this.throughput = new CSVWriter(new FileWriter(name + "_" + run + "_throughput.csv", true));
            throughput.writeNext(new String[]{
                    name,
                    run,
                    thread,
                    String.valueOf(partitions),
                    String.valueOf(experiment_window),
                    String.valueOf(startProc),
                    String.valueOf(System.currentTimeMillis()),
                    String.valueOf(counter),
                    String.valueOf(contextTime),
                    String.valueOf(originalKey)
            }, false);

            throughput.flush();
            throughput.close();
        }

        private void writeProgressiveMemory(TypedKey<K> key, Runtime runtime, long endtime) {
            Integer originalKey = (Integer) key.getKey();
            memory.writeNext(new String[]{
                    name,
                    run,
                    String.valueOf(partitions),
                    String.valueOf(experiment_window),
                    String.valueOf(runtime.totalMemory()),
                    String.valueOf(runtime.freeMemory()),
                    String.valueOf(System.currentTimeMillis()),
                    String.valueOf(endtime),
                    String.valueOf(observedStreamTime),
                    String.valueOf(counter),
                    String.valueOf(originalKey)
            }, false);

            try {
                memory.flush();
            } catch (IOException e) {

            }
        }

        @Override
        public boolean isActive(K key) {
            return true;
        }
    }
}
