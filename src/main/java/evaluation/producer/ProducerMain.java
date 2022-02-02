package evaluation.producer;

import evaluation.ExperimentsConfig;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.log4j.Log4j;
import utils.KEPLrCMDParameters;

import java.io.IOException;
import java.util.IllegalFormatFlagsException;
import java.util.Properties;
import java.util.stream.IntStream;

import static evaluation.ExperimentsConfig.EXPERIMENT_NAME;

@Log4j
public class ProducerMain {
    public static void main(String[] args){
        Properties properties = KEPLrCMDParameters.parseProducerParams(args);
        WProducerBase base;

        System.out.println(properties);

        switch (properties.getProperty(EXPERIMENT_NAME)){
            case "W1":
                base = new W1Producer(properties);
                break;
            case "W2":
                base = new W2Producer(properties);
                break;
            case "W3":
                base = new W3Producer(properties);
                break;
            case "W4":
                base = new W4Producer(properties);
                break;
            default:
                throw new IllegalFormatFlagsException("Name of the Experiment not provided.");
        }

        int partitionIndexMax = Integer.parseInt(properties.getProperty(ExperimentsConfig.EXPERIMENT_PARTITION_MAX_INDEX));
        int partitionIndexMin = Integer.parseInt(properties.getProperty(ExperimentsConfig.EXPERIMENT_PARTITION_MIN_INDEX));

        try {
            base.setupSchemas();
            base.createRecords(IntStream.range(partitionIndexMin, partitionIndexMax+1).toArray());
        } catch (IOException | RestClientException e) {
            e.printStackTrace();
        }
    }
}
