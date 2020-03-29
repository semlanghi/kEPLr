package evaluation.keplr;


import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


public class W2 extends WBase {
    private static Logger LOGGER = LoggerFactory.getLogger(W2.class);
    public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
        setup(args);
        createStream();
        LOGGER.info("OUTPUT ON " + output_topic);
        //every A followed by B
        typedStreams[0].times(1).every().followedBy(typedStreams[1].times(1), within).chunk().throughput(app_supplier).to(output_topic);
        startStream();
    }
}
