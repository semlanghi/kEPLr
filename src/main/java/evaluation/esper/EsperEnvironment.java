package evaluation.esper;

import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.internal.collection.Pair;
import com.espertech.esper.compiler.client.EPCompileException;
import com.espertech.esper.runtime.client.EPDeployException;
import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import utils.KEPLrCMDParameters;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static evaluation.ExperimentsConfig.EXPERIMENT_NAME;
import static evaluation.ExperimentsConfig.EXPERIMENT_RUN;

public class EsperEnvironment {

    public static final String query = " create context Keyed " +
            " coalesce  by  consistent_hash_crc32(key)  from  Key  granularity 3  preallocate; \n" +
            " @name('Prova')context Keyed " +
            " select x.start_time, y.end_time, x.idA, y.idB " +
            " from pattern[every x=A -> every y=B where timer:within(60000 milliseconds)]; ";

    public static void main(String[] args) throws IOException, EPCompileException, EPDeployException {

        Properties properties = KEPLrCMDParameters.parseEsperWorkerParams(args);
        Configuration configuration = new Configuration();
        EsperBase base;

        switch (properties.getProperty(EXPERIMENT_NAME)){
            case "W1":
                base = new EsperW1(configuration, properties.getProperty(ExperimentsConfig.EXPERIMENT_RUN));
                break;
            case "W2":
                base = new EsperW2(configuration, properties.getProperty(ExperimentsConfig.EXPERIMENT_RUN));
                break;
            case "W3":
                base = new EsperW3(configuration, properties.getProperty(ExperimentsConfig.EXPERIMENT_RUN));
                break;
            case "W4":
                base = new EsperW4(configuration, properties.getProperty(ExperimentsConfig.EXPERIMENT_RUN));
                break;
            default:
                throw new IllegalFormatFlagsException("Name of the Experiment not provided.");
        }

        SchemaRegistryClient schemaRegistryClient = base.setup(properties);
        base.deployQueries(new ParametricQueries(Long.parseLong(properties.getProperty(ExperimentsConfig.EXPERIMENT_WINDOW))));
        EsperCustomAdapter<GenericRecord, GenericRecord> adapter = new KafkaAvroEsperCustomAdapter<>(base.getDumpingListener(), properties, base.getEpRuntime().getEventService(), schemaRegistryClient);
        adapter.process(genericRecord -> new Pair<>(genericRecord, (Long) genericRecord.get("end_time")));
    }

    static Schema loadSchema(final String name) throws IOException {
        try (
                final InputStream input = EsperEnvironment.class
                        .getClassLoader()
                        .getResourceAsStream(name)
        ) {
            return new Schema.Parser().parse(input);
        }
    }
}
