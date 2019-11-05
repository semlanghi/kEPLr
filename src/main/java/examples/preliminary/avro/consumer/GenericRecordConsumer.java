package examples.preliminary.avro.consumer;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.Properties;

public class GenericRecordConsumer {

    public static void main(String[] args){

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "examples.preliminary.avro.serde");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);

        consumer.subscribe(Arrays.asList("followed_by_stream"));
        try{
            while(true){
                ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
                for(ConsumerRecord<String,GenericRecord> record : records){
                    System.out.println("key: "+record.key()+" value: "+record.value());
                }
            }
        }finally {
            consumer.close();
        }


    }


}
