package examples;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import keplr.etype.EType;
import keplr.etype.ETypeString;
import keplr.ktstream.KTStream;
import org.apache.kafka.streams.kstream.KStream;
import utils.MyTimestampExtractor;

import java.util.Properties;
import java.util.UUID;

/**
 * First example of KEPLr's DSL using String-String pairs.
 */
public class MainStringExample {

    public static void main(String[] args) throws InterruptedException {


        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "prova_windowstore_"+ UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class);

        EType<String,String> type1 = new ETypeString("Smoke");
        EType<String,String> type2 = new ETypeString("TemperatureEvent");

        EType [] types = {type1,type2};

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> stream = builder.stream("input");
        KTStream<String,String>[] typedStreams = KTStream.match(stream,types);


        typedStreams[0].times(1).followedBy(typedStreams[1].times(1), 5L,
                (value1, value2) -> value1+"_followedBy_"+value2).every().to("output_final1");

        Topology topo = builder.build(config);

        System.out.println(topo);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

    }

}
