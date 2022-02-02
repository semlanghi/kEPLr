package evaluation.keplr;

import evaluation.producer.*;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import utils.KEPLrCMDParameters;

import java.io.IOException;
import java.util.IllegalFormatFlagsException;
import java.util.Properties;

import static evaluation.ExperimentsConfig.EXPERIMENT_NAME;

public class WorkerMain {

    public static void main(String[] args){
        Properties properties = KEPLrCMDParameters.parseWorkerParams(args);
        WBase base;

        switch (properties.getProperty(EXPERIMENT_NAME)){
            case "W1":
                base = new W1(properties);
                break;
            case "W2":
                base = new W2(properties);
                break;
            case "W3":
                base = new W3(properties);
                break;
            case "W4":
                base = new W4(properties);
                break;
            default:
                throw new IllegalFormatFlagsException("Name of the Experiment not provided.");
        }

        try {
            base.setup();
            base.createStream();
            base.startStream();
        } catch (IOException | RestClientException e) {
            e.printStackTrace();
        }
    }
}
