package evaluation.consumer;

import evaluation.keplr.WBase;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import keplr.etype.ETypeAvro;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;


/**
 * Consumer used to count the events from a certain topic.
 */
public class CorrectnessCount {
    static String ab = "ab";
    static SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(ab);
    private static Schema schemaA;
    private static Schema schemaB;

    public static void main(String[] args) throws IOException, RestClientException {


        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        String topic = "topic-W1-chunkn-30-chunks-100-chunkg-0";
        consumer.subscribe(Arrays.asList(topic));
        long numberOfRecords = 0L;


        schemaA = loadSchema("A.asvc");
        schemaB = loadSchema("B.asvc");

        ETypeAvro a = new ETypeAvro(schemaA);
        ETypeAvro b = new ETypeAvro(schemaB);

        ETypeAvro product = (ETypeAvro) a.product(b, true);

        Schema ab = product.getSchema();

        schemaRegistryClient.register("A", schemaA);
        schemaRegistryClient.register("B", schemaB);
        schemaRegistryClient.register("AB", ab);


        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
                for (ConsumerRecord<String, String> record : records) {
                    numberOfRecords++;
                    System.out.println(numberOfRecords);
                }

            }
        } finally {
            consumer.close();
        }


    }

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = WBase.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
