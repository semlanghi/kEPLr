package evaluation.producer;

import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import lombok.extern.log4j.Log4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Abstract class extended by all the producers.
 * The records are grouped into chunks of fixed {@link WProducerBase#chunkSize},
 * that sets up the initial size of the chunk.
 * Then, the {@link WProducerBase#numberOfChunks} sets up the number of chunks that should
 * be created and so the length of the experiment.
 * The {@link WProducerBase#growthSize} sets the increment in size of a successive chunk with
 * respect to the predecessor.
 *
 * <---------------------WITHIN-------------------><---------------------WITHIN------------------->
 * <-INITIAL_CHUNK_SIZE->                          <-INITIAL_CHUNK_SIZE-><-GROWTH_SIZE->
 *
 * @see W1Producer,W2Producer,W3Producer,W4Producer
 */

@Log4j
public abstract class WProducerBase extends KafkaProducer<Integer, GenericRecord>{


    protected final int chunkSize;
    protected final int numberOfChunks;
    protected final int growthSize;
    protected int window;

    protected final int partitions;
    protected long aCount = 0;
    protected long bCount = 0;
    protected long simulatedTime;
    protected long ID = 0;

    private final String topic;
    private GenericRecordBuilder typeARecordBuilder;
    private GenericRecordBuilder typeBRecordBuilder;
    private GenericRecordBuilder typeEndRecordBuilder;
    private final SchemaRegistryClient schemaRegistryClient;

    public WProducerBase(Properties properties) {
        super(properties);
        partitions = Integer.parseInt(properties.getProperty(ExperimentsConfig.EXPERIMENT_PARTITION_COUNT,"1"));
        topic = properties.getProperty(ExperimentsConfig.EXPERIMENT_INPUT_TOPIC, properties.getProperty(ExperimentsConfig.EXPERIMENT_NAME) +"-"+partitions);
        chunkSize = Integer.parseInt(properties.getProperty(ExperimentsConfig.EXPERIMENT_CHUNK_SIZE)); //Mandatory Field
        numberOfChunks = Integer.parseInt(properties.getProperty(ExperimentsConfig.EXPERIMENT_NUM_CHUNKS)); //Mandatory Field
        growthSize = Integer.parseInt(properties.getProperty(ExperimentsConfig.EXPERIMENT_CHUNK_GROWTH,"0"));
        window = Integer.parseInt(properties.getProperty(ExperimentsConfig.EXPERIMENT_WINDOW)); //Mandatory Field
        simulatedTime = 0L;
        if(properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG).startsWith("mock")){
            schemaRegistryClient = MockSchemaRegistry.getClientForScope(ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL.split("//")[1]);
        }else {
            String baseUrl = properties.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
            schemaRegistryClient = new CachedSchemaRegistryClient(baseUrl, 4);
        }


        /*
        We do not support contemporaneity. Thus, two events must have different timestamp.
        At maximum one event per millisecond. If chunckSize grater than window, some events in the chunk might go outside
        the window.
         */
        if(this.window < this.chunkSize){
            this.window = chunkSize;
        }
    }


    public void setupSchemas() throws IOException, RestClientException {
        Schema schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
        Schema schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);
        Schema schemaEND = loadSchema(ExperimentsConfig.EVENT_SCHEMA_END);
        typeARecordBuilder = new GenericRecordBuilder(schemaA);
        typeBRecordBuilder = new GenericRecordBuilder(schemaB);
        typeEndRecordBuilder = new GenericRecordBuilder(schemaEND);


        if(schemaRegistryClient.testCompatibility("A", schemaA))
            schemaRegistryClient.register("A", schemaA);//, 0, 1);
        if(schemaRegistryClient.testCompatibility("B", schemaB))
            schemaRegistryClient.register("B", schemaB);//, 0, 2);
        if(schemaRegistryClient.testCompatibility("END", schemaEND))
            schemaRegistryClient.register("END", schemaEND);//, 0, 3);
    }

    protected void sendRecordA(long id, long time, boolean end, int key) {
        typeARecordBuilder.set("idA", id);
        typeARecordBuilder.set("start_time", time);
        typeARecordBuilder.set("end_time", time);
        typeARecordBuilder.set("end", end);
        typeARecordBuilder.set("partition", "KEY-" + key);

        sendRecord(typeARecordBuilder.build(), key);
        aCount++;
    }

    protected void sendRecordB(long id, long time, boolean end, int key) {
        typeBRecordBuilder.set("idB", id);
        typeBRecordBuilder.set("start_time", time);
        typeBRecordBuilder.set("end", end);
        typeBRecordBuilder.set("end_time", time);
        typeBRecordBuilder.set("partition", "KEY-" + key);

        sendRecord(typeBRecordBuilder.build(), key);
        bCount++;
    }

    protected void sendRecordEND(long id, long time, int key) {
        typeEndRecordBuilder.set("idEND", id);
        typeEndRecordBuilder.set("start_time", time);
        typeEndRecordBuilder.set("end_time", time);

        sendRecord(typeEndRecordBuilder.build(), key);
    }

    private void sendRecord(GenericData.Record record, int key) {
        send(new ProducerRecord<>(topic, key, record));
        flush();
    }

    public void createRecords(int[] keyset){
        log.info("Total number of chunks: " + numberOfChunks);

        log.info("Starting time: " + simulatedTime);

        /*
        Iterate until the last chunk, that will be emitted qith an END event at the end.
         */
        int i;
        for (i = 0; i < numberOfChunks ; i++) {
            sendBatch(keyset, i);
            simulatedTime+=window;
        }
        sendLastBatch(keyset, i);
    }

    protected void sendBatch(int[] keyset, int i) {
        for (int value : keyset) {
            int currentChunkSize = chunkSize + growthSize * i;
            createKeyedBatch(currentChunkSize, value);
            System.out.println("Created chunk number: " + (i + 1) + " for partition " + value);
        }
    }

    protected void sendLastBatch(int[] keyset, int i){
        for (int value : keyset) {
            int currentChunkSize = chunkSize + growthSize * i;
            createKeyedLastBatch(currentChunkSize, value);
            System.out.println("Created chunk number: " + (i + 1) + " for partition " + value);
        }
    }

    protected abstract void createKeyedLastBatch(int currentChunkSize, int key);

    protected abstract void createKeyedBatch(int currentChunkSize, int key);

    private Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = ExperimentsConfig.class.getClassLoader().getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
