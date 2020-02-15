package evaluation.esper;

import com.espertech.esper.common.client.EPCompiled;
import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.configuration.common.ConfigurationCommonEventTypeAvro;
import com.espertech.esper.common.client.util.EventTypeBusModifier;
import com.espertech.esper.common.internal.event.avro.EventTypeAvroHandler;
import com.espertech.esper.common.internal.event.avro.EventTypeAvroHandlerFactory;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPDeployException;
import com.espertech.esper.runtime.client.EPDeployment;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import producer.GenericRecordProducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class EsperEnvironment {

    public static final String query = " create context Keyed " +
            " coalesce  by  consistent_hash_crc32(key)  from  Key  granularity 3  preallocate; \n" +
            " @name('Prova')context Keyed " +
            " select x.start_time, y.end_time, x.idA, y.idB " +
            " from pattern[every x=A -> every y=B where timer:within(60000 milliseconds)]; ";

    public static void main(String[] args) throws IOException, EPCompileException, EPDeployException {

        Properties props = new Properties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "examples.preliminary.avro.serde"+ UUID.randomUUID());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, Serdes.String().deserializer().getClass());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("auto.offset.reset", "earliest");

        Properties producerConfig = new Properties();

        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        producerConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().serializer().getClass());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        Configuration config = new Configuration();

        //config.getCompiler().getByteCode().setAllowSubscriber(true);
        config.getCompiler().getByteCode().setAccessModifiersPublic();
        config.getCompiler().getByteCode().setBusModifierEventType(EventTypeBusModifier.BUS);


        String output_topic = "output_final_esper_part_1";

        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<String, GenericRecord>(producerConfig);

        //ConfigurationCommonEventTypeAvro avroTypeA = new ConfigurationCommonEventTypeAvro();
        //avroTypeA.setAvroSchema(loadSchema("A.asvc"));


        //config.getCommon().addEventTypeAvro("A", avroTypeA);

        //EventTypeAvroHandler handler = new EventTypeAvroHandlerBase

        //config.getCommon().setTransientConfiguration("esper-avro", new EventTypeAvroHandlerFactory());
        Map<String,Object> mapA = new HashMap<>();
        mapA.put("start_time",  Long.class);
        mapA.put("end_time", Long.class);
        mapA.put("idA", Long.class);


        Map<String,Object> mapB = new HashMap<>();
        mapB.put("start_time",  Long.class);
        mapB.put("end_time", Long.class);
        mapB.put("idB", Long.class);


        Map<String,Object> mapKey = new HashMap<>();
        mapKey.put("key", String.class);
        String[] supertypes = {"Key"};

        config.getCommon().addEventType("Key", mapKey );
        config.getCommon().addEventType("A", mapA, supertypes);
        config.getCommon().addEventType("B", mapB, supertypes);



        EPRuntime runtime = EPRuntimeProvider.getRuntime("", config);

        EPCompiler compiler = EPCompilerProvider.getCompiler();

        CompilerArguments compilerArguments = new CompilerArguments();
        compilerArguments.setConfiguration(config);

        EPCompiled compiled = compiler.compile(query, compilerArguments);

        EPDeployment deployment = runtime.getDeploymentService().deploy(compiled);




        runtime.getDeploymentService().getStatement(deployment.getDeploymentId(), "Prova").addListener(new ProducerListener(producer, output_topic));


        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);

        String inputTopic = "AB_input_part_1";
        consumer.subscribe(Arrays.asList(inputTopic));
        Map<String,Object> map = new HashMap<>();
        try{
            while(true){
                ConsumerRecords<String, GenericRecord> records = consumer.poll(5000);
                for(ConsumerRecord<String,GenericRecord> record : records){
                    //System.out.println("key: "+record.key()+" value: "+record.value());
                    System.out.println("input event");
                    if(record.value().getSchema().getName().equals("A")){
                        map.put("start_time",  (Long) record.value().get("start_time"));
                        map.put("end_time", (Long) record.value().get("end_time"));
                        map.put("idA", (Long) record.value().get("idA"));
                        map.put("key", (String) record.key());


                    }else {
                        map.put("start_time", (Long) record.value().get("start_time"));
                        map.put("end_time", (Long) record.value().get("end_time"));
                        map.put("idB", (Long) record.value().get("idB"));
                        map.put("key", (String) record.key());
                    }

                    runtime.getEventService().sendEventMap(map, record.value().getSchema().getName());

                }
            }
        }finally {
            consumer.close();
        }


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
