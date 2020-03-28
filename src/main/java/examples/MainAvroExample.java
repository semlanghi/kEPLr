package examples;

import org.apache.kafka.streams.*;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.ETypeAvro;
import org.apache.kafka.streams.keplr.ktstream.KTStream;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import utils.AvroTimestampExtractor;
import utils.KafkaAvroSerDe;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiConsumer;

public class MainAvroExample {

    public static void main(String[] args) throws InterruptedException, IOException {


        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "prova_windowstore_"+ UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        //config.put(KTStreamsConfig.DEFAULT_INSTANCEOF_CLASS_CONFIG, EqualsPredicateInstanceof.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);


        EType<String,GenericRecord> type1 = new ETypeAvro(loadSchema("A.asvc"));
        EType<String,GenericRecord> type2 = new ETypeAvro(loadSchema("B.asvc"));

        //EType [] types = {type1,type2};

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,GenericRecord> stream = builder.stream("AB_input_part");
        KTStream<String,GenericRecord>[] typedStreams = KTStream.match(stream, type1,type2);


        typedStreams[0].times(1).every().followedBy(typedStreams[1].times(1).every(), 60000L)
                .to("output_final_part1");

        Topology topo = builder.build(config);
        System.out.println(topo);

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();




        while (true){

            streams.metrics().forEach(new BiConsumer<MetricName, Metric>() {

                @Override
                public void accept(MetricName metricName, Metric metric) {
                    if(metricName.group().contains("evaluation/producer") && metricName.description().equals("The number of outgoing bytes sent to all servers per second") )
                        System.out.println(metricName.group()+" "+metricName.description()+" "+metricName.name()+" and value: " +metric.metricValue());
                }
            });
            //Thread.sleep(500L);
        }

    }

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = MainAvroExample.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
