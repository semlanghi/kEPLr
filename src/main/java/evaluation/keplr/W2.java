package evaluation.keplr;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Properties;

/**
 * Class that sets up Example 2, in EPL:
 *
 * every A -> B within n
 *
 * This example takes all possible event A followed by the first successive
 * event B within n seconds.
 */
public class W2 extends WBase {

    public W2(Properties config) {
        super(config);
    }

    @Override
    protected void completeTopology() {
        typedStreams[0].every()
                .followedBy(typedStreams[1], within)
                .throughput(appSupplier).to(outputTopic);
    }
}
