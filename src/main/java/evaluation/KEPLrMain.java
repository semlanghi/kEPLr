package evaluation;

import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.ETypeAvro;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.apache.kafka.streams.keplr.ktstream.KTStream;
import org.apache.kafka.streams.keplr.ktstream.WrappedKStreamImpl;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import utils.AvroTimestampExtractor;
import utils.KafkaAvroSerDe;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import static evaluation.ExperimentsConfig.SCHEMA_REGISTRY_SCOPE;
import static evaluation.ExperimentsConfig.loadSchema;

public class KEPLrMain {

    public static final String DEFAULT_BOOTSTRAP_SERVER_URL = "localhost:9092";
    public static final String DEFAULT_APPLICATION_PREFIX = "main-default-";
    public static final String DEFAULT_INPUT_TOPIC = "input-topic2";
    public static final String DEFAULT_OUTPUT_TOPIC = "output-topic21";


    public static void main(String[] args) throws IOException, RestClientException {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_PREFIX + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER_URL);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.SCHEMA_REGISTRY_URL);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);

        Schema schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
        Schema schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);

        SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);

        schemaRegistryClient.register("A", schemaA);
        schemaRegistryClient.register("B", schemaB);

        EType<String, GenericRecord> type1 = new ETypeAvro(schemaA);
        EType<String, GenericRecord> type2 = new ETypeAvro(schemaB);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> stream = builder.stream(DEFAULT_INPUT_TOPIC, Consumed.with(Serdes.String(), null));
        WrappedKStreamImpl<String,GenericRecord> wrappedKStream =
                new WrappedKStreamImpl<>((KStreamImpl<String, GenericRecord>) stream);

        KTStream<String,GenericRecord>[] typedStreams =
                wrappedKStream.match(type1, type2);

        long within = 5L;

        typedStreams[0].followedBy(typedStreams[1].every(), within).to(DEFAULT_OUTPUT_TOPIC);


        Topology build = builder.build();

        KafkaStreams streams = new KafkaStreams(build, config);

        streams.start();


    }
}
