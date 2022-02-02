package evaluation.keplr;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Class that sets up Example 3, in EPL:
 *
 * every (A -> B within n)
 *
 * This example takes non-overlapping pairs of event A followed by an
 * event B within n seconds.
 */
public class W3 extends WBase {

    public W3(Properties config) {
        super(config);
    }

    @Override
    protected void completeTopology() {
        typedStreams[0]
                .followedBy(typedStreams[1], within)
                .throughput(appSupplier).to(outputTopic);
    }
}
