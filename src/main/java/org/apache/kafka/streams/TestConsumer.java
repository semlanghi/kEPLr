package org.apache.kafka.streams;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Random;

public class TestConsumer {

    public static void main(String[] args) throws IOException {


        Properties producerConfig = new Properties();

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);



        //final GenericRecordBuilder paymentBuilder =
        //      new GenericRecordBuilder(loadSchema("Payment.avsc"));


        GenericRecordBuilder record = new GenericRecordBuilder(loadSchema("A.asvc"));
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(producerConfig);



        String[] names = {"sammy", "Dio", "Gesu", "Madonna", "Giuseppe"};
        String[] keys = {"key1"};
        Random rng = new Random(12345);
        Random rng2 = new Random(157578);
        Double val;
        Integer index;
        Integer index2;

        for(int i=0; i<60; i++){

            try {
                val = rng.nextDouble();
                index = rng2.nextInt(names.length);
                index2 = rng2.nextInt(keys.length);
                record.set("id", names[index]);

                record.set("amount", val);

                System.out.println("Name: "+names[index]+" Amount: "+val);


                producer.send(new ProducerRecord<String, GenericRecord>("windowing_input", keys[index2], record.build()));
                producer.flush();




                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }



    }

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = TestConsumer.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
