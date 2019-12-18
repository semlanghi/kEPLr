package examples.preliminary.stream;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.state.SessionStore;
import sun.net.www.content.text.Generic;
import utils.KafkaAvroSerDe;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Windowing {

    public static final String INPUT="windowing_input";
    public static final String OUTPUT="windowing_output2";
    public static final Duration INACTIVITY_GAP=Duration.ofSeconds(10);
    public static final String PLAY_EVENTS_PER_SESSION = "play-events-per-session";


    public static void main(String[] args){
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "processor-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, KafkaAvroSerDe.class);

        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");

        KafkaAvroSerDe genericSerDe = new KafkaAvroSerDe();
        genericSerDe.configure(serdeConfig,false);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(INPUT, Consumed.with(Serdes.String(), genericSerDe))
                // group by key so we can count by session windows
                .groupByKey(Grouped.with(Serdes.String(), genericSerDe))
                // window by session
                .windowedBy(SessionWindows.with(INACTIVITY_GAP))
                // count play events per session
                .count(Materialized.<String, Long, SessionStore<Bytes, byte[]>>as(PLAY_EVENTS_PER_SESSION)
                        .withKeySerde(Serdes.String())
                        .withValueSerde(Serdes.Long()))
                // convert to a stream so we can map the key to a string
                .toStream()
                // map key to a readable string
                .map((key, value) -> new KeyValue<>(key.key() + "@" + key.window().start() + "->" + key.window().end(), value))
                // write to play-events-per-session topic
                .to(OUTPUT, Produced.with(Serdes.String(), Serdes.Long()));



        KafkaStreams streams = new KafkaStreams(builder.build(),config);
        streams.start();

    }
}
