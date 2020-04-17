package examples;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
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
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Long().deserializer().getClass());



        KafkaConsumer<String, Long> consumer = new KafkaConsumer<String, Long>(props);

        consumer.subscribe(Arrays.asList("windowing_output2"));
        try{
            while(true){
                ConsumerRecords<String, Long> records = consumer.poll(1000);
                for(ConsumerRecord<String,Long> record : records){
                    System.out.println("key: "+record.key()+" value: "+record.value());
                }
            }
        }finally {
            consumer.close();
        }


    }


}
