package evaluation.keplr;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

/**
 * Class that sets up Example 1, in EPL:
 *
 * every A -> every B within n
 *
 * This example takes all possible event A followed by any possible
 * event B within n seconds.
 */
public class W1 extends WBase {
    private static Logger LOGGER = LoggerFactory.getLogger(W1.class);

    public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
        setup(args);
        createStream();
        LOGGER.info("OUTPUT ON " + output_topic);
        typedStreams[0].times(1).every().followedBy(typedStreams[1].times(1).every(), within).chunk().throughput(app_supplier).to(output_topic);
        startStream();
    }
}
