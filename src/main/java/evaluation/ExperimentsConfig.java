package evaluation;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;

public class ExperimentsConfig {

    private static SchemaRegistryClient registryClient;


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

    public static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = ExperimentsConfig.class.getClassLoader().getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }


}

