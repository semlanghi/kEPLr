package evaluation.keplr;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Class that sets up Example 1, in EPL:
 *
 * every A -> every B within n
 *
 * This example takes all possible event A followed by any possible
 * event B within n seconds.
 */
public class W1 extends WBase {

    public W1(Properties config) {
        super(config);
    }

    @Override
    protected void completeTopology() {
        typedStreams[0].every()
                .followedBy(typedStreams[1].every(), within)
                .throughput(appSupplier).to(outputTopic);
    }
}
