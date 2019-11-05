package examples.preliminary.avro.topology;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Arrays;
import java.util.Properties;

public class AveragesConsumer {

    public static void main(String[] args){

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "examples.preliminary.avro.serde");
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.Double().deserializer().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Double> consumer = new KafkaConsumer<String, Double>(props);

        consumer.subscribe(Arrays.asList("avgs_stream"));
        try{
            while(true){
                ConsumerRecords<String, Double> records = consumer.poll(1000);
                for(ConsumerRecord<String,Double> record : records){
                    System.out.println("name: "+record.key()+" amount: "+record.value());
                }
            }
        }finally {
            consumer.close();
        }


    }
}
