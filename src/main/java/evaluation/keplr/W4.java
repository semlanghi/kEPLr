package evaluation.keplr;

import keplr.ktstream.KTStream;
import org.apache.avro.generic.GenericRecord;

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
    protected KTStream<Integer, GenericRecord> completeTopology() {
        return typedStreams[0]
                .followedBy(typedStreams[1].every(), within);
    }
}
