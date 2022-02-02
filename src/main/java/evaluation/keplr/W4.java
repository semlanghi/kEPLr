package evaluation.keplr;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;


/**
 * Class that sets up Example 4, in EPL:
 *
 * A -> (every B) within n
 *
 * This example takes the first event A followed by any possible
 * event B within n seconds. Then restarts the search.
 */
public class W4 extends WBase {

    public W4(Properties config) {
        super(config);
    }

    @Override
    protected void completeTopology() {
        typedStreams[0]
                .followedBy(typedStreams[1].every(), within)
                .throughput(appSupplier).to(outputTopic);
    }
}
