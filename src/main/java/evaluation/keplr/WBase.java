package evaluation.keplr;

import com.opencsv.CSVWriter;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.keplr.etype.EType;
import org.apache.kafka.streams.keplr.etype.ETypeAvro;
import org.apache.kafka.streams.keplr.ktstream.KTStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import utils.AvroTimestampExtractor;
import utils.KafkaAvroSerDe;

import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.UUID;

public class WBase {

    static String ab = "ab";
    static SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(ab);
    private static Schema schemaA;
    private static Schema schemaB;
    private static Schema schemaEnd;
    private static EType<String, GenericRecord> type1;
    private static EType<String, GenericRecord> type2;
    private static String TOPIC;
    private static Properties config;
    private static StreamsBuilder builder;
    static KTStream<String, GenericRecord>[] typedStreams;

    public static void setup(String topic) throws IOException, RestClientException {
        schemaA = loadSchema("A.asvc");
        schemaB = loadSchema("B.asvc");
        schemaEnd = loadSchema("END.asvc");
        TOPIC = topic;


        schemaRegistryClient.register("A", schemaA);
        schemaRegistryClient.register("B", schemaB);
        schemaRegistryClient.register("END", schemaEnd);


        config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG,
                "prova_windowstore_"+ UUID.randomUUID());
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "localhost:9092");

        config.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + ab);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, KafkaAvroSerDe.class);
        config.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, AvroTimestampExtractor.class);


        type1 = new ETypeAvro(schemaA);
        type2 = new ETypeAvro(schemaB);
    }

    static void createStream(String outputFilePrefix) throws InterruptedException, IOException, RestClientException {
        builder = new StreamsBuilder();

        KStream<String,GenericRecord> stream = builder.stream(TOPIC);
        stream.map(new KeyValueMapper<String, GenericRecord, KeyValue<? extends String,? extends GenericRecord>>() {

            CSVWriter writer = new CSVWriter(new FileWriter(outputFilePrefix+"_"+TOPIC +"output.csv"));
            private long counter=0;
            private long startProc = System.currentTimeMillis();
            @Override
            public KeyValue<? extends String, ? extends GenericRecord> apply(String key, GenericRecord value) {
                counter++;
                if(value.getSchema().equals(schemaEnd)){
                    double diff = System.currentTimeMillis()-startProc;
                    double thr = counter;
                    thr=(thr/diff)*1000;
                    String repr = "Throughput avg: "+thr;
                    writer.writeNext(new String[]{repr});
                    try {
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    System.exit(0);
                }


                return new KeyValue<>(key,value);
            }
        });
        typedStreams = KTStream.match(stream, type1,type2);

    }

    static void createTopology(){
        Topology topo = builder.build(config);
        System.out.println(topo);


    }
    static void startSteam(){
        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
    }

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = WBase.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
