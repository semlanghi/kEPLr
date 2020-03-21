package evaluation.kafkastreams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.ETypeAvro;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.ValueJoiner;
import utils.AvroTimestampExtractor;
import utils.KafkaAvroSerDe;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;
import java.util.function.BiConsumer;

public class MainKafkaStreamsExample {

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


        EType<String, GenericRecord> type1 = new ETypeAvro(loadSchema("A.asvc"));
        EType<String,GenericRecord> type2 = new ETypeAvro(loadSchema("B.asvc"));

        EType [] types = {type1,type2};

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String,GenericRecord> stream = builder.stream("AB_input7");
        //KTStream<String,GenericRecord>[] typedStreams = KTStream.match(stream,types);

        KStream<String,GenericRecord>[] kStreams = stream.branch(types);
        int times=1;

        kStreams[0].filter(new Predicate<String,GenericRecord>() {
            int i=0;
            @Override
            public boolean test(String key, GenericRecord value) {
                i++;
                if(i==times){
                    i=0;
                    return true;
                }else return false;
            }
        }).join(kStreams[1].filter(new Predicate<String, GenericRecord>() {
            int i = 0;

            @Override
            public boolean test(String key, GenericRecord value) {
                i++;
                if (i == times) {
                    i = 0;
                    return true;
                } else return false;
            }
        }), new ValueJoiner<GenericRecord, GenericRecord, GenericRecord>() {
            Schema schema;
            boolean done=false;
            @Override
            public GenericRecord apply(GenericRecord value1, GenericRecord value2) {

                if(!done) {
                    schema = SchemaBuilder.record(value1.getSchema().getName() + "_X_"+
                            value2.getSchema().getName()).fields()
                            .name("x").type(value1.getSchema()).noDefault()
                            .name("y").type(value2.getSchema()).noDefault()
                            .endRecord();
                    done = true;
                }

                return new GenericRecordBuilder(schema).set("x",value1).set("y", value2).build();
            }
        }, JoinWindows.of(6000).after(60000).before(0)).filter(new Predicate<String, GenericRecord>() {
            @Override
            public boolean test(String key, GenericRecord value) {
                if(value.get("x")!=null && value.get("y")!=null) return true;
                else return false;
            }
        }).to("output_final12");


        Topology topo = builder.build(config);


        System.out.println(topo.describe());

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();




        while (true){

            streams.metrics().forEach(new BiConsumer<MetricName, Metric>() {
                @Override
                public void accept(MetricName metricName, Metric metric) {
                    if(metricName.group().contains("evaluation/producer") && metricName.description().equals("The number of outgoing bytes sent to all servers per second") )//|| metricName.name().contains("latency"))
                        System.out.println(metricName.group()+" "+metricName.description()+" "+metricName.name()+" and value: " +metric.metricValue());
                }
            });
            //Thread.sleep(15000L);
        }



    }

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = MainKafkaStreamsExample.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
