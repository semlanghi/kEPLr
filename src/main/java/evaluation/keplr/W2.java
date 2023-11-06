package evaluation.keplr;

import keplr.ktstream.KTStream;
import org.apache.avro.generic.GenericRecord;

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
    protected KTStream<Integer, GenericRecord> completeTopology() {
        return typedStreams[0].every()
                .followedBy(typedStreams[1], within);
    }
}
