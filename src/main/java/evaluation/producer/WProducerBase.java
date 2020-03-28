package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public abstract class WProducerBase {
    static int WITHIN;
    static int INITIAL_CHUNK_SIZE;
    static int NUMBER_OF_CHUNKS;
    static int GROWTH_SIZE;


    static String BOOTSTRAP_SERVER_URL = "localhost:9092";
    static String TOPIC;
    static int PARTITIONS;
    static long A_COUNT = 0;
    static long B_COUNT = 0;

    static GenericRecordBuilder typeARecordBuilder;
    static GenericRecordBuilder typeBRecordBuilder;
    static GenericRecordBuilder typeEndRecordBuilder;
    static KafkaProducer<String, GenericRecord> producer;
    static final String ab = "ab";
    static final String SCHEMA_REGISTRY_URL = "mock://" + ab;
    static SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(ab);
    private static Schema schemaA;
    private static Schema schemaEND;
    private static Schema schemaB;


    protected static void setup(String[] args) throws IOException, RestClientException {
        schemaA = loadSchema("A.asvc");
        schemaB = loadSchema("B.asvc");
        schemaEND = loadSchema("END.asvc");
        typeARecordBuilder = new GenericRecordBuilder(schemaA);
        typeBRecordBuilder = new GenericRecordBuilder(schemaB);
        typeEndRecordBuilder = new GenericRecordBuilder(schemaEND);
        schemaRegistryClient.register("A", schemaA);
        schemaRegistryClient.register("B", schemaB);
        schemaRegistryClient.register("END", schemaEND);
        producer = new KafkaProducer<>(getProducerConfig());

        TOPIC = args[0];
        PARTITIONS = Integer.parseInt(args[1]);
        INITIAL_CHUNK_SIZE = Integer.parseInt(args[2]);
        NUMBER_OF_CHUNKS = Integer.parseInt(args[3]);
        GROWTH_SIZE = Integer.parseInt(args[4]);
        WITHIN = Integer.parseInt(args[5]);
    }

    static void createRecordA(long id, long time) {
        typeARecordBuilder.set("id", id);
        typeARecordBuilder.set("start_time", time);
        typeARecordBuilder.set("end_time", time);
        sendRecord(typeARecordBuilder.build());
    }

    static void createRecordB(long id, long time) {
        typeBRecordBuilder.set("id", id);
        typeBRecordBuilder.set("start_time", time);
        typeBRecordBuilder.set("end_time", time);
        sendRecord(typeBRecordBuilder.build());
    }

    static void sendEndRecord(long id) {
        typeEndRecordBuilder.set("id", id);
        typeEndRecordBuilder.set("A_count", A_COUNT);
        typeEndRecordBuilder.set("B_count", B_COUNT);
        for (int i = 0; i < PARTITIONS; i++) {
            typeEndRecordBuilder.set("partition", i);
            producer.send(new ProducerRecord<>(TOPIC, i, String.valueOf(id), typeEndRecordBuilder.build()));
        }
    }

    private static void sendRecord(GenericData.Record record) {
        for (int i = 0; i < PARTITIONS; i++) {
            producer.send(new ProducerRecord<>(TOPIC, i, String.valueOf(i), record));
            producer.flush();
            if (record.getSchema().equals(schemaA)) A_COUNT++;
            else if (record.getSchema().equals(schemaB)) B_COUNT++;
        }
    }

    private static Schema loadSchema(final String name) throws IOException {
        try (final InputStream input = WProducerBase.class
                .getClassLoader()
                .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }

    private static Properties getProducerConfig() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
//        producerConfig.put(ProducerConfig.ACKS_CONFIG, "1");
        return producerConfig;
    }
}
