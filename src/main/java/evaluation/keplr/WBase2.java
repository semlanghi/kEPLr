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
public class WBase2 {

    static String ab = "ab";
    static SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(ab);
    private static Schema schemaEnd;
    private static String TOPIC;
    public static Properties config;
    private static StreamsBuilder builder;
    static KTStream<String, GenericRecord>[] typedStreams;
    static KafkaProducer<String, GenericRecord> producer;
    private static final String BOOTSTRAP_SERVER_URL = "localhost:9092";
    public static String output_topic;
    public static KafkaStreams streams;

    private static Logger LOGGER = LoggerFactory.getLogger(WBase2.class);
    public static GenericRecordBuilder measurementBuilder;
    public static long within;
    protected static ApplicationSupplier app_supplier;


    /**
     * This method takes all the arguments of the {@link W1#main(String[])} method,
     * from all the Wn classes. Also, it creates all the configuration objects.
     *
     * @param args
     * @return
     * @throws IOException
     * @throws RestClientException
     * @see Properties
     * @see ExperimentsConfig
     */
    public static String setup(String[] args) throws IOException, RestClientException {

        config = new Properties();
        UUID run = UUID.randomUUID();
        //TOPIC = args[0];
        TOPIC = "TEST_TOPIC";
        output_topic = "output_" + TOPIC;

        LOGGER.info("RUNNING EXPERIMENT " + TOPIC);
        //String broker_count = args[1];
        String broker_count = "3";
        //within = Long.parseLong(args[2]);

        app_supplier = new ApplicationSupplier(Integer.parseInt(broker_count));

        config.put(ExperimentsConfig.EXPERIMENT_NAME, TOPIC);
        config.put(ExperimentsConfig.EXPERIMENT_RUN, run.toString());
        config.put(ExperimentsConfig.EXPERIMENT_OUTPUT, ExperimentsConfig.EXPERIMENT_OUTPUT_DEFAULT);
        config.put(ExperimentsConfig.EXPERIMENT_BROKER_COUNT, broker_count);
        //config.put(ExperimentsConfig.EXPERIMENT_WINDOW, args[3]);

        schemaEnd = loadSchema(ExperimentsConfig.EVENT_SCHEMA_END);

        schemaRegistryClient.register("END", schemaEnd);//, 0, 3);

        //config.put(StreamsConfig.APPLICATION_ID_CONFIG, args[4]);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.SCHEMA_REGISTRY_URL);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);
        config.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, broker_count);


        return output_topic;
    }
}
