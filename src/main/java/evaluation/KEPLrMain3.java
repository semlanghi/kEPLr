package evaluation;

import evaluation.keplr.WBase2;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.ETypeAvro;
import org.apache.kafka.streams.keplr.ktstream.KTStream;
import org.apache.kafka.streams.keplr.ktstream.WrappedKStreamImpl;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.internals.KStreamImpl;
import utils.AvroTimestampExtractor;
import utils.KafkaAvroSerDe;

import java.io.FileWriter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

import static evaluation.ExperimentsConfig.SCHEMA_REGISTRY_SCOPE;
import static evaluation.ExperimentsConfig.loadSchema;

public class KEPLrMain3 {

    public static final String DEFAULT_BOOTSTRAP_SERVER_URL = "localhost:9092";
    public static final String DEFAULT_APPLICATION_PREFIX = "main-default-";
    public static final String DEFAULT_INPUT_TOPIC = "input-topic1";
    public static final String DEFAULT_OUTPUT_TOPIC = "output-topic21";


    public static void main(String[] args) throws IOException, RestClientException {
        String input_topic = args[0];
        int broker_count = Integer.parseInt(args[1]);
        long within = Long.parseLong(args[2]);
        int run_id = Integer.parseInt(args[3]);
        String followed_by_case = "A->every(B)";

        /*
        String input_topic = DEFAULT_INPUT_TOPIC;
        long within = 5L;
        int broker_count = 3;
        int run_id = 0;
         */


        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, DEFAULT_APPLICATION_PREFIX + UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVER_URL);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.SCHEMA_REGISTRY_URL);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);


        Schema schemaA = loadSchema("schemas/e0.asvc");
        Schema schemaB = loadSchema("schemas/e1.asvc");
        Schema schemaC = loadSchema("schemas/e2.asvc");

        SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);

        schemaRegistryClient.register("A", schemaA);
        schemaRegistryClient.register("B", schemaB);
        schemaRegistryClient.register("C", schemaC);

        EType<String, GenericRecord> type1 = new ETypeAvro(schemaA);
        EType<String, GenericRecord> type2 = new ETypeAvro(schemaB);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, GenericRecord> stream = builder.stream(input_topic, Consumed.with(Serdes.String(), null));
        WrappedKStreamImpl<String, GenericRecord> wrappedKStream =
            new WrappedKStreamImpl<>((KStreamImpl<String, GenericRecord>) stream);

        KTStream<String, GenericRecord>[] typedStreams =
            wrappedKStream.match(type1, type2);


        typedStreams[0].followedBy(typedStreams[1].every(), within).to(DEFAULT_OUTPUT_TOPIC);


        Topology build = builder.build();

        KafkaStreams streams = new KafkaStreams(build, config);
        //for (MetricName key : streams.metrics().keySet()) {
        //    System.out.println(key);
        //}
        ArrayList<String> stats = new ArrayList<>();
        stats.add("run_id:" + run_id);
        stats.add("broker_count:" + broker_count);
        stats.add("start_time:" + LocalDateTime.now());
        stats.add("followed_by_case:" + followed_by_case);
        


        //streams.start();
        //TODO ask samuele about ending stream based on specific event

        stats.add("end_time:" + LocalDateTime.now());


        FileWriter writer = new FileWriter("output.txt");
        for (String str : stats) {
            writer.write(str + System.lineSeparator());
        }
        writer.close();


    }
}
