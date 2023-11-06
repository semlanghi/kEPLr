package utils;

import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.internals.DefaultStreamPartitioner;

import java.util.Arrays;
import java.util.IllegalFormatFlagsException;
import java.util.Properties;
import java.util.UUID;


public class KEPLrCMDParameters {

    public static Properties parseProducerParams(String[] args) throws IllegalFormatFlagsException {
        Properties props = new Properties();
        System.out.println(Arrays.toString(args));
        for (int i = 0; i < args.length; i++) {
            String tmp = args[i];

            switch (tmp){
                case "--broker":
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, args[++i]);
                    break;
                case "--name":
                    props.put(ExperimentsConfig.EXPERIMENT_NAME, args[++i]);
                    break;
                case "--topic":
                    props.put(ExperimentsConfig.EXPERIMENT_INPUT_TOPIC, args[++i]);
                    break;
                case "--partitions-count":
                    props.put(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT, args[++i]);
                    break;
                case "--chunk-size":
                    props.put(ExperimentsConfig.EXPERIMENT_CHUNK_SIZE, args[++i]);
                    break;
                case "--chunk-growth":
                    props.put(ExperimentsConfig.EXPERIMENT_CHUNK_GROWTH, args[++i]);
                    break;
                case "--chunk-number":
                    props.put(ExperimentsConfig.EXPERIMENT_NUM_CHUNKS, args[++i]);
                    break;
                default:
                    throw new IllegalFormatFlagsException("Parameter "+tmp+" is not well formatted.");
            }
        }

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.Integer().serializer().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 500000000L);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL);

        return props;
    }

    public static Properties parseWorkerParams(String[] args) throws IllegalFormatFlagsException {
        Properties props = new Properties();
        for (int i = 0; i < args.length; i++) {
            String tmp = args[i];
            System.out.println(Arrays.toString(args));

            switch (tmp){
                case "--broker":
                    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[++i]);
                    break;
                case "--name":
                    props.put(ExperimentsConfig.EXPERIMENT_NAME, args[++i]);
                    break;
                case "--run":
                    props.put(ExperimentsConfig.EXPERIMENT_RUN, args[++i]);
                    break;
                case "--topic":
                    props.put(ExperimentsConfig.EXPERIMENT_INPUT_TOPIC, args[++i]);
                    break;
                case "--output":
                    props.put(ExperimentsConfig.EXPERIMENT_OUTPUT_TOPIC, args[++i]);
                    break;
                case "--partitions-count":
                    props.put(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT, args[++i]);
                    break;
                case "--broker-count":
                    props.put(ExperimentsConfig.EXPERIMENT_BROKER_COUNT, args[++i]);
                    break;
                case "--chunk-size":
                    props.put(ExperimentsConfig.EXPERIMENT_CHUNK_SIZE, args[++i]);
                    break;
                case "--chunk-growth":
                    props.put(ExperimentsConfig.EXPERIMENT_CHUNK_GROWTH, args[++i]);
                    break;
                case "--chunk-number":
                    props.put(ExperimentsConfig.EXPERIMENT_NUM_CHUNKS, args[++i]);
                    break;
                case "--within":
                    props.put(ExperimentsConfig.EXPERIMENT_WINDOW, args[++i]);
                    break;
                default:
                    throw new IllegalFormatFlagsException("Parameter "+tmp+" is not well formatted.");
            }
        }

        String appName = props.getProperty(ExperimentsConfig.EXPERIMENT_NAME) + "-" +
                props.getProperty(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT) + "-" +
                props.getProperty(ExperimentsConfig.EXPERIMENT_RUN) + UUID.randomUUID().toString();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                props.getProperty(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL);

        return props;
    }

    public static Properties parseEsperWorkerParams(String[] args) throws IllegalFormatFlagsException {
        Properties props = new Properties();
        for (int i = 0; i < args.length; i++) {
            String tmp = args[i];
            System.out.println(Arrays.toString(args));

            switch (tmp){
                case "--broker":
                    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, args[++i]);
                    break;
                case "--name":
                    props.put(ExperimentsConfig.EXPERIMENT_NAME, args[++i]);
                    break;
                case "--run":
                    props.put(ExperimentsConfig.EXPERIMENT_RUN, args[++i]);
                    break;
                case "--topic":
                    props.put(ExperimentsConfig.EXPERIMENT_INPUT_TOPIC, args[++i]);
                    break;
                case "--output":
                    props.put(ExperimentsConfig.EXPERIMENT_OUTPUT_TOPIC, args[++i]);
                    break;
                case "--partitions-count":
                    props.put(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT, args[++i]);
                    break;
                case "--broker-count":
                    props.put(ExperimentsConfig.EXPERIMENT_BROKER_COUNT, args[++i]);
                    break;
                case "--chunk-size":
                    props.put(ExperimentsConfig.EXPERIMENT_CHUNK_SIZE, args[++i]);
                    break;
                case "--chunk-growth":
                    props.put(ExperimentsConfig.EXPERIMENT_CHUNK_GROWTH, args[++i]);
                    break;
                case "--chunk-number":
                    props.put(ExperimentsConfig.EXPERIMENT_NUM_CHUNKS, args[++i]);
                    break;
                case "--within":
                    props.put(ExperimentsConfig.EXPERIMENT_WINDOW, args[++i]);
                    break;
                default:
                    throw new IllegalFormatFlagsException("Parameter "+tmp+" is not well formatted.");
            }
        }

        String appName = props.getProperty(ExperimentsConfig.EXPERIMENT_NAME) + "-" +
                props.getProperty(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT) + "-" +
                props.getProperty(ExperimentsConfig.EXPERIMENT_RUN) + UUID.randomUUID().toString();

        UUID uuid = UUID.randomUUID();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, uuid.toString());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, uuid.toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                props.getProperty(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT));
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL);

        return props;
    }

}
