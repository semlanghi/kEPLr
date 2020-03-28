package evaluation;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;

public class ExperimentsConfig {

    private static SchemaRegistryClient registryClient;
    private static Schema schemaA;
    private static Schema schemaB;
    private static Schema schemaEnd;
    private static Schema measurement;


    public static final String EVENT_SCHEMA_A = "A.asvc";
    public static final String EVENT_SCHEMA_B = "B.asvc";
    public static final String EVENT_SCHEMA_AB = "AB.asvc";
    public static final String EVENT_SCHEMA_Measurement = "Measurement.asvc";
    public static final String EVENT_SCHEMA_END = "END.asvc";

    public static final String EXPERIMENT_NAME = "experiment.name";
    public static final String EXPERIMENT_RUN = "experiment.run";
    public static final String EXPERIMENT_OUTPUT = "experiment.output";
    public static final String EXPERIMENT_BROKER_COUNT = "experiment.broker_count";
    public static final String EXPERIMENT_MAX_CHUNK_SIZE = "experiment.max_chunk_size";
    public static final String EXPERIMENT_CHUNK_SIZE = "experiment.chunk_size";
    public static final String EXPERIMENT_INIT_CHUNK_SIZE = "experiment.init_chunk_size";
    public static final String EXPERIMENT_NUM_CHUNKS = "experiment.num_chunk_size";
    public static final String EXPERIMENT_CHUNK_GROWTH = "experiment.chunks_growth";
    public static final String EXPERIMENT_OUTPUT_DEFAULT = "throughput.csv";
    public static final String EXPERIMENT_WINDOW = "experiment.within";

    public static final String RECORD_NAME = "name";
    public static final String RECORD_RUN = "run";
    public static final String RECORD_start_time = "start_time";
    public static final String RECORD_end_time = "end_time";
    public static final String RECORD_BROKER_COUNT = "broker_count";
    public static final String RECORD_RECORDS_COUNT = "records_count";
    public static final String RECORD_CHUNK_SIZE = "chunk_size";
    public static final String RECORD_INIT_CHUNK_SIZE = "init_chunk_size";
    public static final String RECORD_NUM_CHUNKS = "num_chunks";
    public static final String RECORD_CHUNK_GROWTH = "chunks_growth";
    public static final String RECORD_WINDOW = "within";
    public static final String RECORD_PARTITION = "partition";
    public static final String RECORD_THREAD = "thread";

    public static final String SCHEMA_REGISTRY_URL = "mock://ab";

    public static SchemaRegistryClient getRegistry() {
        if (registryClient == null)
            registryClient = MockSchemaRegistry.getClientForScope(SCHEMA_REGISTRY_URL);
        return registryClient;
    }

    public static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = ExperimentsConfig.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }


    private static void setup() throws IOException, RestClientException {
        schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
        schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);
        schemaEnd = loadSchema(ExperimentsConfig.EVENT_SCHEMA_END);
        measurement = loadSchema(ExperimentsConfig.EVENT_SCHEMA_Measurement);

        registryClient.register("A", schemaA);//, 0, 1);
        registryClient.register("B", schemaB);//, 0, 2);
        registryClient.register("END", schemaEnd);//, 0, 3);
        registryClient.register("Measurement", measurement);//, 0, 4);
    }
}

