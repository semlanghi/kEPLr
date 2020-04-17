package evaluation.keplr;


import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class that sets up Example 3, in EPL:
 *
 * every (A -> B within n)
 *
 * This example takes non-overlapping pairs of event A followed by an
 * event B within n seconds.
 */
public class W3 extends WBase {
    private static Logger LOGGER = LoggerFactory.getLogger(W2.class);

    public static void main(String[] args) throws InterruptedException, IOException, RestClientException {

        setup(args);
        createStream();
        LOGGER.info("OUTPUT ON " + output_topic);
        typedStreams[0].times(1).followedBy(typedStreams[1].times(1) , within).every().chunk().throughput(app_supplier).to(output_topic);

        startStream();

    }
}
