package evaluation.producer;

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

public class ABABProducer {

    private static final String BOOTSTRAP_SERVER_URL = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String TOPIC   = "AB_input_part_1";
    private static final String KEY = "key1";
    private static long ID = 0;
    private static final int CHUNK_SIZE = 60000;

    private static GenericRecordBuilder typeARecordBuilder;
    private static GenericRecordBuilder typeBRecordBuilder;
    private static KafkaProducer<String, GenericRecord> producer;


    /**
     * Creates sequential records (pairs) of type A and type B in fixed "time chunks"
     * Each pair contains of:
     * record A with idA: X and start_time=end_time=Y
     * record B with idB: X and start_time=end_time=Y+1
     * Every new chunk has double the size (number of pairs) of previous chunk
     * ToDo: Add support for multiple partitions
     */

    public static void main(String[] args) throws IOException {
        typeARecordBuilder = new GenericRecordBuilder(loadSchema("A.asvc"));
        typeBRecordBuilder = new GenericRecordBuilder(loadSchema("B.asvc"));
        producer = new KafkaProducer<>(getProducerConfig());

        int numberOfChunks = calculateNumberOfCunks();

        System.out.println("Total number of chunks: " + numberOfChunks);
        for (int i = 0; i < numberOfChunks; i++) {
            for (int j = 0; j < Math.pow(2, 1+i); j+=2) {
                long simulatedTime = 1 + i* CHUNK_SIZE + j;
                createSequentialAB(simulatedTime);
            }
            System.out.println("Created chunk number: " + (i + 1));
        }

    }
    private static void createSequentialAB(long time){
        createRecordA(ID, time);
        createRecordB(ID++, time + 1);
    }

    private static void createRecordA(long id, long time){
        typeARecordBuilder.set("idA", id);
        typeARecordBuilder.set("start_time", time);
        typeARecordBuilder.set("end_time", time);
        sendRecord(typeARecordBuilder.build());
    }

    private static void createRecordB(long id, long time){
        typeBRecordBuilder.set("idB", id);
        typeBRecordBuilder.set("start_time", time);
        typeBRecordBuilder.set("end_time", time);
        sendRecord(typeBRecordBuilder.build());
    }

    private static void sendRecord(GenericData.Record record){
        producer.send(new ProducerRecord<>(TOPIC, KEY, record));
        producer.flush();
    }


    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = GenericRecordProducer.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }

    private static int calculateNumberOfCunks(){
        return (int) Math.floor( Math.log(CHUNK_SIZE) / Math.log(2));
    }

    private static Properties getProducerConfig(){
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER_URL);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return producerConfig;
    }
}

