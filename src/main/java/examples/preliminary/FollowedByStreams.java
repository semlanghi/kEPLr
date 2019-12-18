package examples.preliminary;

import io.confluent.kafka.serializers.*;
import operators.followedBy.FollowedByProcessor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import utils.KafkaAvroSerDe;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class FollowedByStreams {

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


        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");


        // serializers for examples.preliminary.avro
        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(serdeConfig, false);

        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(serdeConfig, false);

        KafkaAvroDeserializer deserializer2 = new KafkaAvroDeserializer();
        deserializer2.configure(serdeConfig, false);

        KafkaAvroSerDe serDe = new KafkaAvroSerDe();
        serDe.configure(serdeConfig,true);

        KafkaAvroSerDe serDe2 = new KafkaAvroSerDe();
        serDe2.configure(serdeConfig,true);

        StoreBuilder builder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("events_following"), serDe, Serdes.Integer());


        Topology topoBuilder = new Topology();
        topoBuilder.addSource("messages-source",
                Serdes.String().deserializer(),
                deserializer,
                "topic_followed_by_3")
                .addSource("messages_source_2", Serdes.String().deserializer(),
                        deserializer2,
                        "topic_followed_by_7")
                .addProcessor("selector-processor",
                        () -> new FollowedByProcessor("topic_followed_by_3", "topic_followed_by_7"),
                        "messages-source","messages_source_2")
                .addStateStore(builder, "selector-processor")
                .addSink("final-sink",
                        "followed_by_stream",
                        Serdes.String().serializer(),
                        serDe2.serializer(),
                        "selector-processor");

        KafkaStreams streams = new KafkaStreams(topoBuilder, config);
        streams.start();
    }
}
