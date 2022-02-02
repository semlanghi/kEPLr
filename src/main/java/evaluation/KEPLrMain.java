package evaluation;

import evaluation.keplr.ApplicationSupplier;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import keplr.etype.EType;
import keplr.etype.ETypeAvro;
import keplr.ktstream.KTStream;
import keplr.ktstream.WrappedKStreamImpl;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import utils.AvroTimestampExtractor;
import utils.KafkaAvroSerDe;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

import static evaluation.ExperimentsConfig.MOCK_SCHEMA_REGISTRY_SCOPE;

public class KEPLrMain {

    public static final String DEFAULT_EXPERIMENT_NAME = "exp";
    public static final long DEFAULT_EXPERIMENT_RUN = 4L;

    public static final String DEFAULT_BOOTSTRAP_SERVER_URL = "localhost:9092";
    public static final String DEFAULT_APPLICATION_PREFIX = DEFAULT_EXPERIMENT_NAME+"-"+DEFAULT_EXPERIMENT_RUN+"-";
    public static final String DEFAULT_INPUT_TOPIC = "input-topic-"+DEFAULT_EXPERIMENT_NAME+"-"+DEFAULT_EXPERIMENT_RUN;
    public static final String DEFAULT_OUTPUT_TOPIC = "output-topic"+DEFAULT_EXPERIMENT_NAME+"-"+DEFAULT_EXPERIMENT_RUN;

    public static void main(String[] args) throws IOException, RestClientException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_PREFIX + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER_URL);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);
        config.put(ExperimentsConfig.EXPERIMENT_WINDOW, 5L);
        config.put(ExperimentsConfig.EXPERIMENT_NAME, DEFAULT_EXPERIMENT_NAME);
        config.put(ExperimentsConfig.EXPERIMENT_RUN, DEFAULT_EXPERIMENT_RUN);

        Schema schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
        Schema schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);
        Schema schemaEnd = loadSchema(ExperimentsConfig.EVENT_SCHEMA_END);

        SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(MOCK_SCHEMA_REGISTRY_SCOPE);

        schemaRegistryClient.register("A", schemaA);
        schemaRegistryClient.register("B", schemaB);
        schemaRegistryClient.register("END", schemaEnd);


        EType<String, GenericRecord> type1 = new ETypeAvro(schemaA);
        EType<String, GenericRecord> type2 = new ETypeAvro(schemaB);
        EType<String, GenericRecord> typeEnd = new ETypeAvro(schemaEnd);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> stream = builder.stream(DEFAULT_INPUT_TOPIC, Consumed.with(Serdes.String(), null));
        WrappedKStreamImpl<String,GenericRecord> wrappedKStream =
                new WrappedKStreamImpl<>(stream);

        KTStream<String,GenericRecord>[] typedStreams =
                wrappedKStream.match(type1, type2, typeEnd);

        long within = 5L;

        ApplicationSupplier app_supplier = new ApplicationSupplier(1);

        typedStreams[0].followedBy(typedStreams[1].every(), within).merge(typedStreams[2]).throughput(app_supplier).to(DEFAULT_OUTPUT_TOPIC);

        Topology build = builder.build();

        KafkaStreams streams = new KafkaStreams(build, config);

        app_supplier.setApp(streams);

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

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = ExperimentsConfig.class.getClassLoader().getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
