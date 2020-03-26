package evaluation.keplr;


import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class W4 extends WBase{

    public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
        setup("W4");
        createStream(args[0]);
        typedStreams[0].times(1).followedBy(typedStreams[1].times(1).every(), 5000L)
                .every().to("output_final");

        createTopology();
        startSteam();

    }
}
