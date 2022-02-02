package evaluation.producer;

import evaluation.ExperimentsConfig;
import evaluation.KEPLrMain;
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
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.Future;

import static evaluation.ExperimentsConfig.*;

@Log4j
public class InteractiveProducerMain {

    public static void main(String[] args){
        CustomSingleTopicProducer customSingleTopicProducer = new CustomSingleTopicProducer(getDefaultProps(), KEPLrMain.DEFAULT_INPUT_TOPIC);
        customSingleTopicProducer.addSchema("A","A.asvc");
        customSingleTopicProducer.addSchema("B","B.asvc");
        customSingleTopicProducer.addSchema("END","END.asvc");


        int i = 0, j=0, z=0, k=0;
        Scanner scanner = new Scanner(System.in);
        String nextLine, nextkey;
        Set<String> keys = new HashSet<>();

        do {
            System.out.println("Event to send?");
            nextLine = scanner.nextLine();
            System.out.println(nextLine);
            if (nextLine.equalsIgnoreCase("a")){
                System.out.println("key?");
                nextkey = scanner.nextLine();
                keys.add(nextkey);
                customSingleTopicProducer.sendRecord(nextkey,"A", i++, j++, false );
            }

            if (nextLine.equalsIgnoreCase("b")){
                System.out.println("key?");
                nextkey = scanner.nextLine();
                keys.add(nextkey);
                customSingleTopicProducer.sendRecord(nextkey,"B", z++, j++, false );
            }

            if (nextLine.equalsIgnoreCase("end")){
                for (String key: keys
                     ) {
                    customSingleTopicProducer.sendEndRecord(key,"END", k++, j++ );
                }
            }

        } while(!nextLine.equalsIgnoreCase("end"));

        scanner.close();
    }

    static Properties getDefaultProps(){
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KEPLrMain.DEFAULT_BOOTSTRAP_SERVER_URL);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return producerConfig;
    }

    private static class CustomSingleTopicProducer extends KafkaProducer<String,GenericRecord>{
        private final Map<String,GenericRecordBuilder> recordBuilderMap;
        private final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(MOCK_SCHEMA_REGISTRY_SCOPE);
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

        public Future<RecordMetadata> sendEndRecord(String key, String typeName, long id, long time){
            GenericRecordBuilder temp = recordBuilderMap.get(typeName);
            temp.set("idEND", id);
            temp.set("start_time", time);
            temp.set("end_time", time);

            return send(new ProducerRecord<>(topic,key,temp.build()));
        }

        private Schema loadSchema(final String name) throws IOException {
            try (
                    final InputStream input = ExperimentsConfig.class.getClassLoader().getResourceAsStream(name)
            ) {
                return new Schema.Parser().parse(input);
            }
        }
    }


}
