package evaluation.keplr;


import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;


public class W2 extends WBase{
    public static void main(String[] args) throws InterruptedException, IOException, RestClientException {
        setup("W2");
        createStream(args[0]);

        typedStreams[0].times(1).every().followedBy(typedStreams[1].times(1), Long.parseLong(args[1]))
                .to("output_final");


        createTopology();
        startSteam();
    }
}
