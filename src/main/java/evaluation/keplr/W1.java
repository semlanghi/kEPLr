package evaluation.keplr;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class W1 extends WBase{

    public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
        setup("W1");
        createStream(args[0]);
        typedStreams[0].times(1).every().followedBy(typedStreams[1].times(1).every(), 5000L)
                .to("output_final");

        createTopology();
        startSteam();
    }
}
