package org.apache.kafka.streams;

import producer.GenericRecordProducer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import utils.MyTimestampExtractor;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

public class MainFollowedBy {

    public static void main(String[] args) throws InterruptedException {


        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "prova_windowstore_"+ UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        //config.put(KTStreamsConfig.DEFAULT_INSTANCEOF_CLASS_CONFIG, EqualsPredicateInstanceof.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MyTimestampExtractor.class);

        EType<String,String> type1 = new ETypeString("A");
        EType<String,String> type2 = new ETypeString("B");

        EType [] types = {type1,type2};

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,String> stream = builder.stream("AB_input4");
        KTStream<String,String>[] typedStreams = KTStream.match(stream,types);


        typedStreams[0].times(1).followedBy(typedStreams[1].times(1), 5L,
                new ValueJoiner<String, String, String>() {
                    @Override
                    public String apply(String value1, String value2) {
                        return value1+"_followedBy_"+value2;
                    }
                }).every().to("output_final1");

        Topology topo = builder.build(config);

        System.out.println(topo);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();

        /*while (true){

            streams.metrics().forEach(new BiConsumer<MetricName, Metric>() {
                @Override
                public void accept(MetricName metricName, Metric metric) {
                        if(metricName.equals("process-rate"))
                            System.out.println(metricName.name()+" and value: " +metric.metricValue());
                }
            });
            Thread.sleep(30000L);
        }*/

    }

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = GenericRecordProducer.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
