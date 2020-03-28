package evaluation.keplr;

import com.opencsv.CSVWriter;
import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.ETypeAvro;
import org.apache.kafka.streams.keplr.ktstream.KTStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.AvroTimestampExtractor;
import utils.KafkaAvroSerDe;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static evaluation.ExperimentsConfig.loadSchema;

public class WBase {

    static String ab = "ab";
    static SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(ab);
    private static Schema schemaA;
    private static Schema schemaB;
    private static Schema schemaEnd;
    private static Schema measurement;
    private static EType<String, GenericRecord> type1;
    private static EType<String, GenericRecord> type2;
    private static String TOPIC;
    private static Properties config;
    private static StreamsBuilder builder;
    static KTStream<String, GenericRecord>[] typedStreams;
    static KafkaProducer<String, GenericRecord> producer;
    private static final String BOOTSTRAP_SERVER_URL = "localhost:9092";
    public static String output_topic;
    public static KafkaStreams streams;

    private static Logger LOGGER = LoggerFactory.getLogger(WBase.class);
    public static GenericRecordBuilder measurementBuilder;
    public static long within;

    public static String setup(String[] args) throws IOException, RestClientException {
        config = new Properties();
        UUID run = UUID.randomUUID();
        TOPIC = args[0];
        output_topic = "output_" + TOPIC;

        LOGGER.info("RUNNING EXPERIMENT " + TOPIC);
        String broker_count = args[1];
        String init_chunk_size = args[2];
        String num_chunks = args[3];
        String chunks_groth = args[4];
        within = Long.parseLong(args[5]);

        config.put(ExperimentsConfig.EXPERIMENT_NAME, TOPIC);
        config.put(ExperimentsConfig.EXPERIMENT_RUN, run.toString());
        config.put(ExperimentsConfig.EXPERIMENT_OUTPUT, ExperimentsConfig.EXPERIMENT_OUTPUT_DEFAULT);
        config.put(ExperimentsConfig.EXPERIMENT_BROKER_COUNT, broker_count);
        config.put(ExperimentsConfig.EXPERIMENT_INIT_CHUNK_SIZE, init_chunk_size);
        config.put(ExperimentsConfig.EXPERIMENT_NUM_CHUNKS, num_chunks);
        config.put(ExperimentsConfig.EXPERIMENT_CHUNK_GROWTH, chunks_groth);
        config.put(ExperimentsConfig.EXPERIMENT_WINDOW, args[5]);

        schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
        schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);
        schemaEnd = loadSchema(ExperimentsConfig.EVENT_SCHEMA_END);
        measurement = loadSchema(ExperimentsConfig.EVENT_SCHEMA_Measurement);

        schemaRegistryClient.register("A", schemaA);//, 0, 1);
        schemaRegistryClient.register("B", schemaB);//, 0, 2);
        schemaRegistryClient.register("END", schemaEnd);//, 0, 3);
        schemaRegistryClient.register("Measurement", measurement);//, 0, 4);

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "windowstore_" + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.SCHEMA_REGISTRY_URL);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, Integer.parseInt(broker_count));

        type1 = new ETypeAvro(schemaA);
        type2 = new ETypeAvro(schemaB);

//        ETypeAvro product = (ETypeAvro) type1.product(type2, false);


//        schemaRegistryClient.register("C", product.getSchema());//, 0, 5);
        return output_topic;

    }

    static void createStream() throws InterruptedException, IOException, RestClientException {
        builder = new StreamsBuilder();

        KStream<String, GenericRecord> stream = builder.stream(TOPIC);
        String property = config.getProperty(ExperimentsConfig.EXPERIMENT_OUTPUT);
        stream.map(new KeyValueMapper<String, GenericRecord, KeyValue<? extends String, ? extends GenericRecord>>() {

            CSVWriter writer = new CSVWriter(new FileWriter(property, true));
            private long counter = 0;
            private long startProc = System.currentTimeMillis();

            @Override
            public KeyValue<? extends String, ? extends GenericRecord> apply(String key, GenericRecord value) {
                counter++;
                if (value.getSchema().equals(schemaEnd)) {
                    streams.localThreadsMetadata().forEach(threadMetadata -> {
                        String thread = threadMetadata.threadName();
                        Integer partition = (Integer) value.get("partition");
                        Object b_count = value.get("B_count");
                        Object a_count = value.get("A_count");
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
                                String.valueOf(a_count), String.valueOf(b_count), String.valueOf(partition), thread
                        }, false);
                        buildMeasurement(startProc, counter, a_count, b_count, partition, thread);

                    });

                    try {
                        writer.flush();
                        streams.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }

                return new KeyValue<>(key, value);
            }


        });

        typedStreams = KTStream.match(stream, type1, type2);

    }

    static void createTopology() {
        Topology topo = builder.build(config);
        System.out.println(topo.describe());
    }

    static void startStream() {
        streams = new KafkaStreams(builder.build(), config);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {

            Runtime.getRuntime().halt(0);
        }));

        streams.setStateListener((newState, oldState) -> {
            if (KafkaStreams.State.PENDING_SHUTDOWN.equals(newState)) {
                try {
                    // setup a timer, so if nice exit fails, the nasty exit happens
                    sendOut("END");
                    Thread.sleep(60000);
                    Runtime.getRuntime().exit(0);
                } catch (Throwable ex) {
                    // exit nastily if we have a problem
                    Runtime.getRuntime().halt(-1);
                } finally {
                    // should never get here
                    Runtime.getRuntime().halt(-1);
                }
            }
        });


        streams.start();

    }


    private static void buildMeasurement(long counter, long start_, Object a_count, Object b_count, int partition, String thread) {

        measurementBuilder = new GenericRecordBuilder(measurement);
        measurementBuilder.set(ExperimentsConfig.RECORD_NAME, config.getProperty(ExperimentsConfig.EXPERIMENT_NAME));
        measurementBuilder.set(ExperimentsConfig.RECORD_RUN, config.getProperty(ExperimentsConfig.EXPERIMENT_RUN));
        measurementBuilder.set(ExperimentsConfig.RECORD_start_time, start_);
        measurementBuilder.set(ExperimentsConfig.RECORD_end_time, System.currentTimeMillis());
        measurementBuilder.set(ExperimentsConfig.RECORD_BROKER_COUNT, Integer.parseInt(config.getProperty(ExperimentsConfig.EXPERIMENT_BROKER_COUNT)));
        measurementBuilder.set(ExperimentsConfig.RECORD_INIT_CHUNK_SIZE, Integer.parseInt(config.getProperty(ExperimentsConfig.EXPERIMENT_INIT_CHUNK_SIZE)));
        measurementBuilder.set(ExperimentsConfig.RECORD_NUM_CHUNKS, Integer.parseInt(config.getProperty(ExperimentsConfig.EXPERIMENT_NUM_CHUNKS)));
        measurementBuilder.set(ExperimentsConfig.RECORD_CHUNK_GROWTH, Integer.parseInt(config.getProperty(ExperimentsConfig.EXPERIMENT_CHUNK_GROWTH)));
        measurementBuilder.set(ExperimentsConfig.RECORD_RECORDS_COUNT, counter);
        measurementBuilder.set(ExperimentsConfig.RECORD_WINDOW, Long.parseLong(config.getProperty(ExperimentsConfig.EXPERIMENT_WINDOW)));
        measurementBuilder.set(ExperimentsConfig.RECORD_PARTITION, partition);
        measurementBuilder.set(ExperimentsConfig.RECORD_THREAD, thread);
        measurementBuilder.set("A_count", a_count);
        measurementBuilder.set("B_count", b_count);


    }

    private static void sendOut(String key) {
        LOGGER.info("Sending out END");
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.SCHEMA_REGISTRY_URL);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
//
        producer = new KafkaProducer<>(producerConfig);

        producer.send(new ProducerRecord<>(output_topic, key, measurementBuilder.build()));
    }
}
