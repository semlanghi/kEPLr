package evaluation.esper;


import com.espertech.esper.common.client.configuration.Configuration;
import com.espertech.esper.common.client.configuration.common.ConfigurationCommonEventTypeAvro;
import com.espertech.esper.compiler.client.CompilerArguments;
import com.espertech.esper.compiler.client.EPCompiler;
import com.espertech.esper.compiler.client.EPCompilerProvider;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPRuntimeProvider;
import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.avro.Schema;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static evaluation.ExperimentsConfig.EXPERIMENT_NAME;
import static evaluation.esper.EsperEnvironment.loadSchema;

public abstract class EsperBase {

    protected Configuration config;
    protected EPRuntime epRuntime;
    protected EPCompiler epCompiler;
    protected CompilerArguments arguments;
    protected Map<String, String> deploymentIdMap;
    protected String run;
    protected Schema schemaA;
    protected Schema schemaB;
    protected Schema schemaEnd;
    protected DumpingListener dumpingListener;

    public EsperBase(Configuration configuration, String run) {
        this.config = configuration;
        this.run = run;
        this.deploymentIdMap = new HashMap<>();
    }

    public SchemaRegistryClient setup(Properties props){
        try {
            this.schemaA = loadSchema(ExperimentsConfig.EVENT_SCHEMA_A);
            this.schemaB = loadSchema(ExperimentsConfig.EVENT_SCHEMA_B);
            this.schemaEnd = loadSchema(ExperimentsConfig.EVENT_SCHEMA_END);

            ConfigurationCommonEventTypeAvro avroEventA = new ConfigurationCommonEventTypeAvro(schemaA);
            ConfigurationCommonEventTypeAvro avroEventB = new ConfigurationCommonEventTypeAvro(schemaB);
            ConfigurationCommonEventTypeAvro avroEventEnd = new ConfigurationCommonEventTypeAvro(schemaEnd);

            config.getCommon().addEventTypeAvro("A", avroEventA);
            config.getCommon().addEventTypeAvro("B", avroEventB);
            config.getCommon().addEventTypeAvro("END", avroEventEnd);

            epRuntime = EPRuntimeProvider.getRuntime("", config);
            epCompiler = EPCompilerProvider.getCompiler();

            arguments = new CompilerArguments();
            arguments.setConfiguration(config);

            SchemaRegistryClient schemaRegistryClient;

            if(props.containsKey(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG) && !props.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG).contains("mock")){
                String baseUrl = props.getProperty(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
                schemaRegistryClient = new CachedSchemaRegistryClient(baseUrl, 4);
            }else {
                props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL);
                schemaRegistryClient = MockSchemaRegistry.getClientForScope(ExperimentsConfig.MOCK_SCHEMA_REGISTRY_URL.split("//")[1]);
            }

            schemaRegistryClient.register("A", schemaA, 0, 1);
            schemaRegistryClient.register("B", schemaB, 0, 2);
            schemaRegistryClient.register("END", schemaEnd, 0, 3);

            this.dumpingListener = new DumpingListener(props.getProperty(EXPERIMENT_NAME)+"_"+props.getProperty(ExperimentsConfig.EXPERIMENT_RUN), false, Long.parseLong(props.getProperty(ExperimentsConfig.EXPERIMENT_WINDOW)), props.getProperty(ExperimentsConfig.EXPERIMENT_INPUT_TOPIC));

            return schemaRegistryClient;
        } catch (IOException | RestClientException e) {
            e.printStackTrace();
        }
        return null;
    }

    public DumpingListener getDumpingListener() {
        return dumpingListener;
    }

    public EPRuntime getEpRuntime() {
        return epRuntime;
    }

    public abstract void deployQueries(ParametricQueries parametricQueries);
}
