package evaluation.keplr;

import keplr.ktstream.KTStream;
import org.apache.avro.generic.GenericRecord;

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
    protected KTStream<Integer, GenericRecord> completeTopology() {
        return typedStreams[0]
                .followedBy(typedStreams[1], within);
    }
}
