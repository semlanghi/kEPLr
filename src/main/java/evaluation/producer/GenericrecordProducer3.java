package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
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

public class GenericrecordProducer3 {

    static String ab = "ab";
    static SchemaRegistryClient schemaRegistryClient = MockSchemaRegistry.getClientForScope(ab);


    public static void main(String[] args) throws IOException, RestClientException {

        Schema schemaA = loadSchema("A.asvc");
        Schema schemaB = loadSchema("B.asvc");


        schemaRegistryClient.register("A", schemaA);
        schemaRegistryClient.register("B", schemaB);

        Properties producerConfig = new Properties();

        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://" + ab);
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(producerConfig);

        //final GenericRecordBuilder paymentBuilder =
        //      new GenericRecordBuilder(loadSchema("Payment.avsc"));



        GenericRecordBuilder record = new GenericRecordBuilder(schemaA);


        GenericRecordBuilder record1 = new GenericRecordBuilder(schemaB);



        String[] keys = {"key1","key2","key3","key4"};
        Random rng = new Random(12345);
        Random rng2 = new Random(157578);
        int val;
        Integer index;
        Integer index2=0;
        String topic = "AB_input2";
        long idA=0L;
        long idB=0L;
        long rate = 6000L;
        long currentTime=0;

        long multiplier=5;
        long rest;
        long rateIncr=10;
        int h;
//
//        while(currentTime<600*1000) {
//            System.out.println("iteration");
//            if(1000>=rate) {
//                multiplier = 1000 / rate;

                for (int i = 0; i < rate; i++) {

                    val = rng.nextInt(2);
                    System.out.println("iteration");

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

            /*}

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


            }*/








//
//                //Thread.sleep(1000L);
//
//            currentTime+=1000;
//            rate+=rateIncr;
//        }
//
//        System.out.println(currentTime);
//        System.out.println(rate);




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
