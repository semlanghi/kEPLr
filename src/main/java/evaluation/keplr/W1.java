package evaluation.keplr;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class W1 extends WBase {
    private static Logger LOGGER = LoggerFactory.getLogger(W1.class);

    public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
        setup(args);
        createStream();
        LOGGER.info("OUTPUT ON " + output_topic);
        typedStreams[0].times(1).every().followedBy(typedStreams[1].times(1).every(), Long.parseLong(args[1])).to(output_topic);
        createTopology();
        startStream();
    }
}
