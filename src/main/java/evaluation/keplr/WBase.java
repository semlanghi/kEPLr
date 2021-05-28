package evaluation.keplr;

import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.ETypeAvro;
import org.apache.kafka.streams.keplr.ktstream.KTStream;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.AvroTimestampExtractor;
import utils.KafkaAvroSerDe;

import java.io.IOException;
import java.util.Properties;
import java.util.UUID;

import static evaluation.ExperimentsConfig.loadSchema;


/**
 * Class to setup the examples. All the parameters are set up as static.
 * It is extended by all the Wn classes.
 *
 * @see W1,W2,W3,W4
 */
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
    public static Properties config;
    private static StreamsBuilder builder;
    static KTStream<String, GenericRecord>[] typedStreams;
    static KafkaProducer<String, GenericRecord> producer;
    private static final String BOOTSTRAP_SERVER_URL = "localhost:9092";
    public static String output_topic;
    public static KafkaStreams streams;

    private static Logger LOGGER = LoggerFactory.getLogger(WBase.class);
    public static GenericRecordBuilder measurementBuilder;
    public static long within;
    protected static ApplicationSupplier app_supplier;


    /**
     * This method takes all the arguments of the {@link W1#main(String[])} method,
     * from all the Wn classes. Also, it creates all the configuration objects.
     *
     * @see Properties
     * @see ExperimentsConfig
     * @param args
     * @return
     * @throws IOException
     * @throws RestClientException
     */
    public static String setup(String[] args) throws IOException, RestClientException {


        config = new Properties();
        UUID run = UUID.randomUUID();
        TOPIC = args[0];
        output_topic = "output_" + TOPIC;

        LOGGER.info("RUNNING EXPERIMENT " + TOPIC);
        String broker_count = args[1]; //TODO rename partition count???
        String init_chunk_size = args[2];
        String num_chunks = args[3];
        String chunks_groth = args[4];
        within = Long.parseLong(args[5]);

        app_supplier = new ApplicationSupplier(Integer.parseInt(broker_count));

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

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, args[6]);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.SCHEMA_REGISTRY_URL);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, broker_count);

        type1 = new ETypeAvro(schemaA);
        type2 = new ETypeAvro(schemaB);

        return output_topic;
    }

    /**
     * This method create the {@link KStream} from the {@link WBase#TOPIC} parameter.
     * Then it applies the {@link KTStream#match(KStream, EType[])} operator,
     * creating multiple {@link KTStream}.
     *
     * @see KStream
     * @see KTStream
     * @see StreamsBuilder
     */
    static void createStream() {
        builder = new StreamsBuilder();
        KStream<String, GenericRecord> stream = builder.stream(TOPIC);
        typedStreams = KTStream.match(stream, type1, type2);
    }


    /**
     * Here we build up the {@link Topology} using it to create the
     * {@link KafkaStreams} object.
     * Then, it sets up the stopping of the application through the
     * {@link ApplicationSupplier} object.
     *
     * @see Topology
     * @see ApplicationSupplier
     * @see KafkaStreams
     */
    static void startStream() {

        Topology build = builder.build();

        LOGGER.info(build.describe().toString());

        streams = new KafkaStreams(build, config);

        app_supplier.setApp(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Runtime.getRuntime().halt(0);
        }));

        streams.setStateListener((newState, oldState) -> {
            if (KafkaStreams.State.PENDING_SHUTDOWN.equals(newState)) {
                try {
                    // setup a timer, so if nice exit fails, the nasty exit happens
                    //sendOut("END");
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
}
