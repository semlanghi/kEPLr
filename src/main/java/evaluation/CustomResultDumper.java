package evaluation;

import com.opencsv.CSVWriter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.keplr.etype.ETypeAvro;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import static evaluation.ExperimentsConfig.SCHEMA_REGISTRY_SCOPE;
import static evaluation.ExperimentsConfig.loadSchema;


/**
 * A dumper used to dump the result in a file. Representing the whole
 * event, together with the actual partition from which the
 * event comes from.
 */

@Log4j
public class CustomResultDumper {

    public static void main(String[] args) throws IOException, RestClientException {

        SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
        Schema schemaAB;

        Schema schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
        Schema schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);

        schemaRegistryClient.register("A", schemaA, 0, 1);
        schemaRegistryClient.register("B", schemaB, 0, 2);

        schemaAB = ((ETypeAvro) new ETypeAvro(schemaA).product(new ETypeAvro(schemaB), true)).getSchema();
        schemaRegistryClient.register("A_X_B", schemaAB, 0, 3);

        Properties props = new Properties();
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KEPLrMain.DEFAULT_BOOTSTRAP_SERVER_URL);


        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(KEPLrMain.DEFAULT_OUTPUT_TOPIC));

        consumer.poll(0);

        while (true) {
            ConsumerRecords<String, GenericRecord> poll = consumer.poll(Duration.ofMillis(500));
            poll.forEach(record -> {
                    GenericRecord value = record.value();
                    Schema schema = value.getSchema();
                    if (schemaAB.equals(schema)) {
                        GenericRecord x = (GenericRecord) value.get("x");
                        GenericRecord y = (GenericRecord) value.get("y");
//                        String[] header = {"start_time", "end_time", "idA", "start_timeA", "end_timeA", "idB", "start_timeB", "end_timeB"};
                        String[] nextLine = {"AXB",
                                String.valueOf(value.get("start_time")),
                                String.valueOf(value.get("start_time")),
                                String.valueOf(value.get("end_time")),
                                String.valueOf(value.get("end")),
                                String.valueOf(x.get("idA")),
                                String.valueOf(x.get("start_time")),
                                String.valueOf(x.get("end_time")),
                                String.valueOf(x.get("partition")),
                                String.valueOf(x.get("end")),
                                String.valueOf(y.get("idB")),
                                String.valueOf(y.get("start_time")),
                                String.valueOf(y.get("end_time")),
                                String.valueOf(y.get("partition")),
                                String.valueOf(y.get("end"))};
                        log.info(Arrays.toString(nextLine));
                    }else {
                        log.warn("Not the right schema");
                    }
            });
        }
    }

}