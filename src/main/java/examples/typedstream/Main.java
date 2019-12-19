package examples.typedstream;

import examples.preliminary.avro.producer.GenericRecordProducer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import operators.followedBy.FollowedByProcessor;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.joda.time.Minutes;
import utils.KafkaAvroSerDe;

import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws IOException {

        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "processor-kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");
        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);



        // Create the state stores. We need one for each of the
        // MessageProcessor's in the topology.


        final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");


        // serializers for examples.preliminary.avro
        /*KafkaAvroSerializer serializer = new KafkaAvroSerializer();
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


        KafkaStreams streams = new KafkaStreams(topoBuilder, config);
        streams.start();*/

        final StreamsBuilder builder = new StreamsBuilder();

        KStream<String, GenericRecord> origin = builder.stream("test");

        Schema schema1;
        Schema schema2;

        schema1 = loadSchema("A.asvc");
        schema2 = loadSchema("B.asvc");

        KTStream typedStream = new KTStreamImpl().match(origin, new ETypeSchema(schema1));
        KTStream typedStream2 = new KTStreamImpl().match(origin, new ETypeSchema(schema2));

        Schema resultSchema = SchemaBuilder
                .record("Composed").fields()
                .name("event_1").type(schema1)
                .noDefault().name("event_2").type(schema2).noDefault().endRecord();

        ValueJoiner joiner = new ValueJoiner<GenericRecord, GenericRecord, GenericRecord>() {

            @Override
            public GenericRecord apply(GenericRecord value1, GenericRecord value2) {

                GenericRecordBuilder nwEventsBuilder = new GenericRecordBuilder(resultSchema);

                nwEventsBuilder.set("event_1", value1);
                nwEventsBuilder.set("event_2", value2);

                return nwEventsBuilder.build();
            }
        };

        typedStream.followedBy(typedStream2, joiner, JoinWindows.of(Duration.ofMinutes(1)));


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
