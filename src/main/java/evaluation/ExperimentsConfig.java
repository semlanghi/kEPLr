package evaluation;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;

import java.io.IOException;
import java.io.InputStream;


/**
 * Configuration class to sets up the name of the experiment's properties.
 */
public class ExperimentsConfig {

    public static final String EXPERIMENT_INIT_TIME = "experiment.time.start";
    public static final String EVENT_SCHEMA_A = "A.asvc";
    public static final String EVENT_SCHEMA_B = "B.asvc";
    public static final String EVENT_SCHEMA_AB = "AB.asvc";
    public static final String EVENT_SCHEMA_END = "END.asvc";

    public static final String EXPERIMENT_NAME = "experiment.name";
    public static final String EXPERIMENT_RUN = "experiment.run";
    public static final String EXPERIMENT_INPUT_TOPIC = "experiment.topic.input";
    public static final String EXPERIMENT_OUTPUT_TOPIC = "experiment.topic.output";
    public static final String EXPERIMENT_PARTITION_COUNT = "experiment.partition.count";
    public static final String EXPERIMENT_PARTITION_MAX_INDEX = "experiment.partition.index.max";
    public static final String EXPERIMENT_PARTITION_MIN_INDEX = "experiment.partition.index.min";
    public static final String EXPERIMENT_OUTPUT = "experiment.output";
    public static final String EXPERIMENT_BROKER_COUNT = "experiment.broker_count";
    public static final String EXPERIMENT_CHUNK_SIZE = "experiment.chunk_size";
    public static final String EXPERIMENT_INIT_CHUNK_SIZE = "experiment.init_chunk_size";
    public static final String EXPERIMENT_NUM_CHUNKS = "experiment.num_chunk_size";
    public static final String EXPERIMENT_CHUNK_GROWTH = "experiment.chunks_growth";
    public static final String EXPERIMENT_OUTPUT_DEFAULT = "throughput.csv";
    public static final String EXPERIMENT_WINDOW = "experiment.within";

    public static final String MOCK_SCHEMA_REGISTRY_URL = "mock://ab";
    public static final String MOCK_SCHEMA_REGISTRY_SCOPE = "ab";


}

