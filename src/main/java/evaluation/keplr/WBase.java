package evaluation.keplr;

import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import keplr.etype.EType;
import keplr.etype.ETypeIntAvro;
import keplr.ktstream.KTStream;
import keplr.ktstream.WrappedKStreamImpl;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Class to setupSchemas the examples. All the parameters are set up as static.
 * It is extended by all the Wn classes.
 *
 * @see W1,W2,W3,W4
 */
public abstract class WBase {


    private Schema schemaA;
    private Schema schemaB;
    private Schema schemaEnd;
    private EType<Integer, GenericRecord> type1;
    private EType<Integer, GenericRecord> type2;
    protected Properties config;
    private StreamsBuilder builder;
    protected KTStream<Integer, GenericRecord>[] typedStreams;
    protected KafkaStreams streams;
    protected ApplicationSupplier appSupplier;
    private SchemaRegistryClient schemaRegistryClient;

    protected long within;
    protected String topic;
    protected String outputTopic;
    protected int threadCount;

    public WBase(Properties config) {
        this.config = config;
        this.within = Long.parseLong(config.getProperty(ExperimentsConfig.EXPERIMENT_WINDOW));
        this.topic = config.getProperty(ExperimentsConfig.EXPERIMENT_INPUT_TOPIC);
        this.outputTopic = config.getProperty(ExperimentsConfig.EXPERIMENT_OUTPUT_TOPIC);
        this.threadCount = Integer.parseInt(config.getProperty(StreamsConfig.NUM_STREAM_THREADS_CONFIG));
        if(config.containsKey(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG)){
            String baseUrl = config.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
            schemaRegistryClient = new CachedSchemaRegistryClient(baseUrl, 4);
        }else {
            schemaRegistryClient = MockSchemaRegistry.getClientForScope(ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL.split("//")[1]);
            config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL);
        }
    }

    /**
     * This method takes all the arguments of the experiment,
     * from the main class. Also, it creates all the configuration objects.
     *
     * @see Properties
     * @see ExperimentsConfig
     * @return
     * @throws IOException
     * @throws RestClientException
     */
    public void setup() throws IOException, RestClientException {
        typeSetup();
        appSupplier = new ApplicationSupplier(threadCount);
    }

    private void typeSetup() throws IOException, RestClientException {
        schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
        schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);
        schemaEnd = loadSchema(ExperimentsConfig.EVENT_SCHEMA_END);

        if(schemaRegistryClient.testCompatibility("A", schemaA))
            schemaRegistryClient.register("A", schemaA);//, 0, 1);
        if(schemaRegistryClient.testCompatibility("B", schemaB))
            schemaRegistryClient.register("B", schemaB);//, 0, 2);
        if(schemaRegistryClient.testCompatibility("END", schemaEnd))
            schemaRegistryClient.register("END", schemaEnd);//, 0, 3);

        type1 = new ETypeIntAvro(schemaA);
        type2 = new ETypeIntAvro(schemaB);
    }

    /**
     * This method create the {@link KStream} from the topic defined.
     * Then it applies the {@link KTStream#match(KStream, EType[])} operator,
     * creating multiple {@link KTStream}.
     *
     * @see KStream
     * @see KTStream
     * @see StreamsBuilder
     */
    public void createStream() {
        this.builder = new StreamsBuilder();
        WrappedKStreamImpl<Integer, GenericRecord> wrappedKStream = new WrappedKStreamImpl<>(builder.stream(topic, Consumed.with(Serdes.Integer(), null)));
        typedStreams = wrappedKStream.match(type1, type2);
        completeTopology();
    }

    protected abstract void completeTopology();


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
    public void startStream() {

        Topology build = builder.build();
        streams = new KafkaStreams(build, config);

        appSupplier.setApp(streams);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Runtime.getRuntime().halt(0);
        }));

        streams.setStateListener((newState, oldState) -> {
            if (KafkaStreams.State.PENDING_SHUTDOWN.equals(newState)) {
                try {
                    // setupSchemas a timer, so if nice exit fails, the nasty exit happens
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

    private Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = ExperimentsConfig.class.getClassLoader().getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
