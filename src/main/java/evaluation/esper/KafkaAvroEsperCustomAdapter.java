package evaluation.esper;

import com.espertech.esper.common.client.EventSender;
import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.runtime.client.EPEventService;
import com.opencsv.AbstractCSVWriter;
import com.opencsv.CSVWriter;
import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.logging.Logger;

/**
 * An adapter for consuming events from a specific Kafka Topic.
 * Since the input is coming from Kafka, the input events will arrive
 * as key-value pairs. It contains a {@link KafkaConsumer}, set through the {@link Properties}
 * object. Currently, it support homogeneous files, i.e., files that contain
 * events of the same type, sent through the {@link EventSender} object.
 * Every time an event is sent, the time in the {@link com.espertech.esper.runtime.client.EPRuntime}
 * is advanced accordingly, since we are using the external time by default.
 *
 * @param <K> The key of the Kafka record
 * @param <E> The type of the event sent to Esper
 */

public class KafkaAvroEsperCustomAdapter<K,E> implements EsperCustomAdapter<GenericRecord,E> {

    public static final Logger LOGGER = Logger.getLogger(KafkaAvroEsperCustomAdapter.class.getName());

    private final KafkaConsumer<K,GenericRecord> consumer;
    private final EventSender senderA;
    private final EventSender senderB;
    private int maxEnd;
    private EPEventService epEventService;
    private DumpingListener endingHook;


    /**
     * Constructor for consumption from a Kafka Topic. It consumes the topic continuously until
     * it reaches a specific number of events.
     *  @param props Properties for consumer setting
     * @param epEventService Event Service for advancing Esper time
     * @param schemaRegistryClient
     */
    public KafkaAvroEsperCustomAdapter(DumpingListener endingHook, Properties props, EPEventService epEventService, SchemaRegistryClient schemaRegistryClient) {
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(Collections.singletonList(props.getProperty(ExperimentsConfig.EXPERIMENT_INPUT_TOPIC)));
        this.senderA = epEventService.getEventSender("A");
        this.senderB = epEventService.getEventSender("B");
        this.maxEnd = Integer.parseInt(props.getProperty(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT));
        this.epEventService = epEventService;
        this.endingHook = endingHook;
    }


    @Override
    public void process(Function<GenericRecord, Pair<E,Long>> transformationFunction){
        try{
            /*
            Checks which is the condition tha stop the consumption.
            In case of no stopping criteria, the number of arrived, special ad-hoc "ending events"
            is counted.
             */
            while (!endingHook.getEnd()) {
                ConsumerRecords<K, GenericRecord> records = consumer.poll(Duration.ofSeconds(2));
                records.forEach(record -> {
                    Pair<E, Long> value = transformationFunction.apply(record.value());

                    if (record.value().getSchema().getName().equals("A"))
                        sendA(value);
                    else if (record.value().getSchema().getName().equals("B"))
                        sendB(value);

                    epEventService.advanceTime(value.getSecond());
                });
            }
        } catch (WakeupException e) {
            // Using wakeup to close consumer
        } finally {
            consumer.close();
        }
    }

    private void sendA(Pair<E,Long> eventTimestamp){
        senderA.sendEvent(eventTimestamp.getFirst());
    }

    private void sendB(Pair<E,Long> eventTimestamp){
        senderB.sendEvent(eventTimestamp.getFirst());
    }




}
