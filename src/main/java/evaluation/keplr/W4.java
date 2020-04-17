package evaluation.keplr;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class W4 extends WBase {
    private static Logger LOGGER = LoggerFactory.getLogger(W2.class);

    public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
        setup(args);
        createStream();
        LOGGER.info("OUTPUT ON " + output_topic);
        typedStreams[0].times(1).followedBy(typedStreams[1].every(), within).every().to(output_topic);
        startStream();
    }
}
