package evaluation;

import evaluation.KEPLrMain;
import evaluation.keplr.WBase2;
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
import java.util.concurrent.Future;

import static evaluation.ExperimentsConfig.*;

@Log4j
public class ProducerMain2 extends WBase2 {

    public static void main(String[] args) throws IOException, RestClientException {
        setup(args);
        CustomSingleTopicProducer customSingleTopicProducer = new CustomSingleTopicProducer(getDefaultProps(), KEPLrMain.DEFAULT_INPUT_TOPIC);
        int broker_count = Integer.parseInt(config.getProperty(EXPERIMENT_BROKER_COUNT));


        for (int i = 0; i < broker_count; i++) {
            String name = "schemas/e" + i + ".asvc";
            customSingleTopicProducer.addSchema("e" + i, name);
        }

        int id = 0;
        for (int time = 0; time < 3; ) {
            for (int i = 0; i < broker_count; i++) {
                customSingleTopicProducer.sendRecord(Integer.toString(i), "e" + i, id, time++, false);
            }
            id++;
        }
        //make configurable like evaluation.keplr.W1

    }

    static Properties getDefaultProps() {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KEPLrMain.DEFAULT_BOOTSTRAP_SERVER_URL);
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        return producerConfig;
    }

    private static class CustomSingleTopicProducer extends KafkaProducer<String, GenericRecord> {
        private final Map<String, GenericRecordBuilder> recordBuilderMap;
        private final SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_SCOPE);
        private final String topic;

        public CustomSingleTopicProducer(Properties config, String topic) {
            super(config);
            this.recordBuilderMap = new HashMap<>();
            this.topic = topic;
        }

        public void addSchema(String schemaName, String schemaFile) {
            try {

                Schema temp = loadSchema(schemaFile);
                recordBuilderMap.put(schemaName, new GenericRecordBuilder(temp));
                schemaRegistryClient.register(schemaName, temp);
            } catch (IOException | RestClientException e) {
                e.printStackTrace();
            }
        }

        public Future<RecordMetadata> sendRecord(String key, String typeName, long id, long time, boolean end) {
            GenericRecordBuilder temp = recordBuilderMap.get(typeName);
            temp.set("id" + typeName, id);
            temp.set("start_time", time);
            temp.set("end_time", time);
            temp.set("end", end);
            temp.set("partition", "KEY-0");

            return send(new ProducerRecord<>(topic, key, temp.build()));
        }
    }


}
