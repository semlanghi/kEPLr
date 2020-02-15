package evaluation.consumer;

import com.opencsv.CSVWriter;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

public class GenericRecordConsumer2 {

    public static void main(String[] args) throws IOException {

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id"+UUID.randomUUID().toString());

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);




        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        String prefix = "src/main/resources/";
        String topic = "output_final_esper_part_4";
        consumer.subscribe(Arrays.asList(topic));
        long numberOfRecords=0L;
        long startTime=0;
        long actualTime=0;
        long maxTime=360*1000;
        CSVWriter writer = new CSVWriter(new FileWriter(prefix+topic+"_output.csv"));

        String[] line = new String[2];

        writer.writeNext(line);
        long timeEvent=0;
        try{
            while(actualTime-startTime<=maxTime){
                if(numberOfRecords==0)
                    startTime=System.currentTimeMillis();




                ConsumerRecords<String, GenericRecord> records = consumer.poll(5000);
                for(ConsumerRecord<String, GenericRecord> record : records){
                    //System.out.println("key: "+record.key()+" value: "+record.value());
                    numberOfRecords++;
                    timeEvent= (long) record.value().get("end_time");
                }





                actualTime=System.currentTimeMillis();





                double diff = actualTime-startTime;

                System.out.println("Throughput: "+((double)numberOfRecords)/diff);

                line[0]= String.valueOf(actualTime-startTime);


                line[1]= String.valueOf(((double)numberOfRecords)/diff);

                writer.writeNext(line);

                if(actualTime-startTime>maxTime){
                    System.out.println("event time final :"+timeEvent);
                    return;
                     }


            }
        }finally {
            consumer.close();
        }


    }
}
