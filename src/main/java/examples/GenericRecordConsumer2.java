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

public class GenericRecordConsumer2 {


    static String ab = "ab";
    static SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(ab);

    public static void main(String[] args) throws IOException, RestClientException {

        Schema schemaA = loadSchema("A.asvc");
        Schema schemaB = loadSchema("B.asvc");


        //schemaRegistryClient.register("A", schemaA);
        //schemaRegistryClient.register("B", schemaB);
//        schemaRegistryClient.register("A", loadSchema("A.asvc"));
//        schemaRegistryClient.register("B", loadSchema("B.asvc"));

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "your_client_id"+UUID.randomUUID().toString());

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + ab);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put("auto.offset.reset", "earliest");



        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        String prefix = "src/main/resources/";
        String topic = "output_final";
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




                ConsumerRecords<String, String> records = consumer.poll(5000);
                for(ConsumerRecord<String, String> record : records){
                    System.out.println("key: "+record.key()+" value: "+record.value());
                    numberOfRecords++;
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

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = GenericRecordConsumer2.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
