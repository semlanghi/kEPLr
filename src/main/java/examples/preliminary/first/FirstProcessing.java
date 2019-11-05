package examples.preliminary.first;

import io.confluent.kafka.serializers.*;
import org.apache.avro.Schema;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;


import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

public class FirstProcessing {

    public static void main(String[] args) throws IOException {

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

        StoreBuilder builder = Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("payments-avg"), Serdes.String(), Serdes.Integer());

        /*final Map<String, String> serdeConfig = Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
                "http://localhost:8081");

        KafkaAvroSerializer serializer = new KafkaAvroSerializer();
        serializer.configure(serdeConfig, false);

        KafkaAvroDeserializer deserializer = new KafkaAvroDeserializer();
        deserializer.configure(serdeConfig, false);*/


        // Build the topology.
        Topology topoBuilder = new Topology();
        topoBuilder.addSource("messages-source",
                Serdes.String().deserializer(),
                Serdes.Integer().deserializer(),
                "messages")
                .addProcessor("selector-processor",
                        () -> new kEPLrProcessor(),
                        "messages-source")
                .addStateStore(builder, "selector-processor")
                .addSink("final-sink",
                        "avgs",
                        Serdes.String().serializer(),
                        Serdes.Integer().serializer(),
                        "selector-processor");

        KafkaStreams streams = new KafkaStreams(topoBuilder, config);
        streams.start();

        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,Serdes.Integer().serializer().getClass() );



        //final GenericRecordBuilder paymentBuilder =
        //      new GenericRecordBuilder(loadSchema("Payment.avsc"));


        //GenericRecordBuilder record = new GenericRecordBuilder(loadSchema("Payment.asvc"));
        KafkaProducer<String, Integer> producer = new KafkaProducer<String,Integer>(producerConfig);



        String[] names = {"sammy", "Dio", "Gesu", "Madonna", "Giuseppe"};
        Random rng = new Random(12345);
        Random rng2 = new Random(157578);
        Integer val;
        Integer index;

        for(int i=0; i<60; i++){

            try {
                val = rng.nextInt();
                index = rng2.nextInt(names.length);
                //record.set("id", names[index]);
                //record.set("amount", val);
                System.out.println("Name: "+names[index]+" Amount: "+val);


                producer.send(new ProducerRecord<String, Integer>("messages", names[index], val));
                producer.flush();




                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }


    }

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input =
                        FirstProcessing
                                .class
                                .getClassLoader()
                                .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}

