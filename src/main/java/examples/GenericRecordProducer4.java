package examples;

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

public class GenericRecordProducer4 {

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

        GenericRecordBuilder record1 = new GenericRecordBuilder(loadSchema("B.asvc"));



        String[] keys = {"key1"};
        Random rng = new Random(12345);
        Random rng2 = new Random(157578);
        int val;
        Integer index;
        Integer index2;
        String topic = "AB_input_part";
        long idA=0L;
        long idB=0L;
        long rate = 1000L;
        long currentTime=0;

        long multiplier=0;
        long rest;
        long rateIncr=10;
        int h;

        while(currentTime<180*1000) {
            System.out.println("iteration");
            if(1000>=rate) {
                multiplier = 1000 / rate;

                for (int i = 0; i < rate; i++) {

                    val = rng.nextInt(2);

                    if (val == 0) {



                        index2 = rng2.nextInt(keys.length);

                        // Setting up the record
                        // start_time = end_time
                        record.set("idA", idA++);
                        record.set("start_time", currentTime + multiplier * i);
                        record.set("end_time", currentTime + multiplier * i);




                        //send it to kafka
                        producer.send(new ProducerRecord<String, GenericRecord>(topic, keys[index2], record.build()));
                        producer.flush();
                    } else {
                        //index = rng2.nextInt(names.length);
                        index2 = rng2.nextInt(keys.length);
                        record1.set("idB", idB++);


                        record1.set("start_time", currentTime + multiplier * i);
                        record1.set("end_time", currentTime + multiplier * i);


                        //System.out.println("Name: "+names[index]+" Amount: "+val+" Schema: B");


                        producer.send(new ProducerRecord<String, GenericRecord>(topic, keys[index2], record1.build()));
                        producer.flush();
                    }


                }

            }

            else{
                multiplier = rate/1000;
                rest=rate%1000;

                for (int i = 0; i < 1000; i++) {

                    for (int j = 0; j < multiplier; j++) {
                        val = rng.nextInt(2);

                        if (val == 0) {
                            //index = rng2.nextInt(names.length);
                            index2 = rng2.nextInt(keys.length);
                            record.set("idA", idA++);

                            //record.set("start_time", System.currentTimeMillis());
                            //record.set("end_time", System.currentTimeMillis());

                            record.set("start_time", currentTime + multiplier * i);
                            record.set("end_time", currentTime + multiplier * i);


                            //System.out.println("Name: "+names[index]+" Amount: "+val+" Schema: A");


                            producer.send(new ProducerRecord<String, GenericRecord>(topic, keys[index2], record.build()));
                            producer.flush();
                        } else {
                            //index = rng2.nextInt(names.length);
                            index2 = rng2.nextInt(keys.length);
                            record1.set("idB", idB++);


                            record1.set("start_time", currentTime + multiplier * i);
                            record1.set("end_time", currentTime + multiplier * i);


                            //System.out.println("Name: "+names[index]+" Amount: "+val+" Schema: B");


                            producer.send(new ProducerRecord<String, GenericRecord>(topic, keys[index2], record1.build()));
                            producer.flush();
                        }
                    }
                    if(rest>0){

                        val = rng.nextInt(2);

                        if (val == 0) {
                            //index = rng2.nextInt(names.length);
                            index2 = rng2.nextInt(keys.length);
                            record.set("idA", idA++);

                            //record.set("start_time", System.currentTimeMillis());
                            //record.set("end_time", System.currentTimeMillis());

                            record.set("start_time", currentTime + multiplier * i);
                            record.set("end_time", currentTime + multiplier * i);


                            //System.out.println("Name: "+names[index]+" Amount: "+val+" Schema: A");


                            producer.send(new ProducerRecord<String, GenericRecord>(topic, keys[index2], record.build()));
                            producer.flush();
                        } else {
                            //index = rng2.nextInt(names.length);
                            index2 = rng2.nextInt(keys.length);
                            record1.set("idB", idB++);


                            record1.set("start_time", currentTime + multiplier * i);
                            record1.set("end_time", currentTime + multiplier * i);


                            //System.out.println("Name: "+names[index]+" Amount: "+val+" Schema: B");


                            producer.send(new ProducerRecord<String, GenericRecord>(topic, keys[index2], record1.build()));
                            producer.flush();
                        }
                        rest--;

                    }



                }


            }









            //Thread.sleep(1000L);

            currentTime+=1000;
            //rate+=rateIncr;
        }

        System.out.println(currentTime);
        System.out.println(rate);




    }

    private static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = GenericRecordProducer4.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }

}

