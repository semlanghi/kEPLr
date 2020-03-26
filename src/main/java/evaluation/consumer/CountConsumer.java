package evaluation.consumer;

import com.opencsv.CSVWriter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.avro.Schema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class CountConsumer {



    public static void main(String[] args) throws IOException, RestClientException {


        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id"+UUID.randomUUID().toString());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put("auto.offset.reset", "earliest");






        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        String topic = "output_final";
        consumer.subscribe(Arrays.asList(topic));
        long numberOfRecords=0L;

        try{
            while(true){
                ConsumerRecords<String, String> records = consumer.poll(5000);
                for(ConsumerRecord<String, String> record : records){
                    numberOfRecords++;
                    System.out.println(numberOfRecords);
                }



            }
        }finally {
            consumer.close();
        }


    }
}
