package evaluation;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Future;

import static evaluation.ExperimentsConfig.*;

@Log4j
public class ProducerMain {

    public static void main(String[] args){
        CustomSingleTopicProducer customSingleTopicProducer = new CustomSingleTopicProducer(getDefaultProps(), KEPLrMain.DEFAULT_INPUT_TOPIC);
        customSingleTopicProducer.addSchema("A","A.asvc");
        customSingleTopicProducer.addSchema("B","B.asvc");

        int i = 0, j=0, z=0;
        Scanner scanner = new Scanner(System.in);
        String nextLine, nextkey;

        do {
            System.out.println("Event to send?");
            nextLine = scanner.nextLine();
            System.out.println(nextLine);
            if (nextLine.equalsIgnoreCase("a")){
                System.out.println("key?");
                nextkey = scanner.nextLine();
                customSingleTopicProducer.sendRecord(nextkey,"A", i++, j++, false );
            }

            if (nextLine.equalsIgnoreCase("b")){
                System.out.println("key?");
                nextkey = scanner.nextLine();
                customSingleTopicProducer.sendRecord(nextkey,"B", z++, j++, false );
            }

        } while(!nextLine.equalsIgnoreCase("stop"));

        scanner.close();
    }

    static Properties getDefaultProps(){
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KEPLrMain.DEFAULT_BOOTSTRAP_SERVER_URL);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return producerConfig;
    }

    private static class CustomSingleTopicProducer extends KafkaProducer<String,GenericRecord>{
        private final Map<String,GenericRecordBuilder> recordBuilderMap;
        private final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
        private final String topic;

        public CustomSingleTopicProducer(Properties config, String topic) {
            super(config);
            this.recordBuilderMap = new HashMap<>();
            this.topic = topic;
        }

        public void addSchema(String schemaName ,String schemaFile){
            try {

                Schema temp = loadSchema(schemaFile);
                recordBuilderMap.put(schemaName,new GenericRecordBuilder(temp));
                schemaRegistryClient.register(schemaName, temp);
            } catch (IOException | RestClientException e) {
                e.printStackTrace();
            }
        }

        public Future<RecordMetadata> sendRecord(String key, String typeName, long id, long time, boolean end){
            GenericRecordBuilder temp = recordBuilderMap.get(typeName);
            temp.set("id"+typeName, id);
            temp.set("start_time", time);
            temp.set("end_time", time);
            temp.set("end", end);
            temp.set("partition", "KEY-0");

            return send(new ProducerRecord<>(topic,key,temp.build()));
        }
    }


}
