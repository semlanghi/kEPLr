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

public class nAnBProducer {
    private static final String BOOTSTRAP_SERVER_URL = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String TOPIC   = "nAnB_input_part_1";
    private static final String KEY = "key1";
    private static long ID = 0;
    private static final int CHUNK_SIZE = 600;
    protected static final int NUMBER_OF_CHUNKS = (int) Math.floor( Math.log(CHUNK_SIZE/2.0) / Math.log(2));
    private static final int PARTITIONS = 9;

    protected static GenericRecordBuilder typeARecordBuilder;
    protected static GenericRecordBuilder typeBRecordBuilder;
    protected static KafkaProducer<String, GenericRecord> producer;


    /**
     * Creates sequential n type A and n type B records in fixed "time chunks"
     */
    protected static void setup() throws IOException{
        typeARecordBuilder = new GenericRecordBuilder(loadSchema("A.asvc"));
        typeBRecordBuilder = new GenericRecordBuilder(loadSchema("B.asvc"));
        producer = new KafkaProducer<>(getProducerConfig());
    }
    public static void main(String[] args) throws IOException{
        setup();
        createRecords();
    }

    private static void createRecords(){
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS; i++) {
            long simulatedTime = 1 + i* CHUNK_SIZE;
            createSequentialnAnB((int) Math.pow(2, i+1), simulatedTime);
            System.out.println("Created chunk number: " + (i + 1));
        }
    }

    private static void createSequentialnAnB(int n, long time){
        for (int i = 0; i < n; i++) {
            createRecordA(ID++, time + i);
            createRecordB(ID++, time + i + n);
        }
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
        for (int i = 0; i < PARTITIONS; i++) {
            producer.send(new ProducerRecord<>(TOPIC, String.valueOf(i), record));
            producer.flush();
        }
    }


    private static Schema loadSchema(final String name) throws IOException {
        try (final InputStream input = GenericRecordProducer.class
                        .getClassLoader()
                        .getResourceAsStream(name)) {
            return new Schema.Parser().parse(input);
        }
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