package examples.preliminary.avro.topology;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class AvroStreams {

    public static void main(String[] args){

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "processor-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        //config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, AbstractKafkaAvroSerDe.class);



        // Create the state stores. We need one for each of the
        // MessageProcessor's in the topology.

        StoreBuilder builder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("payments-avgs"), Serdes.String(), Serdes.Double());

        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");

        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(serdeConfig, false);

        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(serdeConfig, false);


        // Build the topology.
        Topology topoBuilder = new Topology();
        topoBuilder.addSource("messages-source",
                Serdes.String().deserializer(),
                deserializer,
                "avro_topic")
                .addProcessor("selector-processor",
                        () -> new AvroProcessor(),
                        "messages-source")
                .addStateStore(builder, "selector-processor")
                .addSink("final-sink",
                        "avgs_stream",
                        Serdes.String().serializer(),
                        Serdes.Double().serializer(),
                        "selector-processor");

        KafkaStreams streams = new KafkaStreams(topoBuilder, config);
        streams.start();
    }
}
