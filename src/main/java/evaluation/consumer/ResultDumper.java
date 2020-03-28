package evaluation.consumer;

import com.opencsv.CSVWriter;
import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
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

import static evaluation.ExperimentsConfig.loadSchema;

public class ResultDumper {
    static final String ab = "ab";
    static final String SCHEMA_REGISTRY_URL = "mock://" + ab;
    static SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(ab);
    private static Schema schemaA;
    private static Schema schemaAB;
    private static Schema schemaEND;
    private static Schema schemaB;
    public static Schema.Parser parser = new Schema.Parser();
    private static Schema measurement;
    public static CSVWriter dumpWriter;
    public static String output_topic;
    public static String input_topic;
    private static CSVWriter reportWriter;

    public static void main(String[] args) throws IOException, RestClientException {

        input_topic = args[0];
        output_topic = "" + args[0];

        dumpWriter = new CSVWriter(new FileWriter(output_topic + "_dump.csv", true));
        reportWriter = new CSVWriter(new FileWriter(output_topic + "_reports.csv", true));


        schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
        schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);
        schemaEND = loadSchema(ExperimentsConfig.EVENT_SCHEMA_END);
        measurement = loadSchema(ExperimentsConfig.EVENT_SCHEMA_Measurement);

        schemaRegistryClient.register("A", schemaA, 0, 1);
        schemaRegistryClient.register("B", schemaB, 0, 2);
        schemaRegistryClient.register("END", schemaEND, 0, 3);
        schemaRegistryClient.register("Measurement", measurement, 0, 4);

        schemaAB = ((ETypeAvro) new ETypeAvro(schemaA).product(new ETypeAvro(schemaB), true)).getSchema();
        schemaRegistryClient.register("C", schemaAB, 0, 5);

        Properties props = new Properties();
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        UUID uuid = UUID.randomUUID();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, uuid.toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, uuid.toString());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(output_topic));

        while (true) {
            ConsumerRecords<String, GenericRecord> poll = consumer.poll(Duration.ofMillis(500));


            poll.forEach(record -> {
                try {
                    GenericRecord value = record.value();
                    Schema schema = value.getSchema();
                    if (schemaAB.equals(schema)) {
                        GenericRecord x = (GenericRecord) value.get("x");
                        GenericRecord y = (GenericRecord) value.get("y");
//                        String[] header = {"start_time", "end_time", "idA", "start_timeA", "end_timeA", "idB", "start_timeB", "end_timeB"};
                        String[] nextLine = {
                                String.valueOf(value.get("start_time")),
                                String.valueOf(value.get("end_time")),
                                String.valueOf(x.get("id")),
                                String.valueOf(x.get("start_time")),
                                String.valueOf(x.get("end_time")),
                                String.valueOf(y.get("id")),
                                String.valueOf(y.get("start_time")),
                                String.valueOf(y.get("end_time"))};
                        write(dumpWriter, nextLine);
                    } else if (schemaA.equals(schema) || schemaB.equals(schema)) {
//                        String[] header = {"id", "start_time", "end_time"};
                        String[] nextLine = {String.valueOf(value.get("id")), String.valueOf(value.get("start_time")), String.valueOf(value.get("end_time"))};
                        write(dumpWriter, nextLine);
                    } else {
//                        String[] header = {"id", "A_count", "B_count"};
                        String[] nextLine = {
                                String.valueOf(value.get("name")),
                                String.valueOf(value.get("start_time")),
                                String.valueOf(value.get("end_time")),
                                String.valueOf(value.get("A_count")),
                                String.valueOf(value.get("B_count")),
                                String.valueOf(value.get("records_count")),
                                String.valueOf(value.get("broker_count")),
                                String.valueOf(value.get("num_chunks")),
                                String.valueOf(value.get("init_chunk_size")),
                                String.valueOf(value.get("chunks_growth")),
                                String.valueOf(value.get("within")),
                                String.valueOf(value.get("partition")),
                                String.valueOf(value.get("thread")),
                        };
                        write(reportWriter, nextLine);
//                        System.exit(0);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }
    }

    private static void write(CSVWriter writer, String[] nextLine) throws IOException {
        writer.writeNext(nextLine, false);
        writer.flush();
    }

}