import evaluation.CustomResultDumper2;
import evaluation.KEPLrMain2;
import evaluation.ProducerMain2;
import evaluation.ProducerMain3;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.junit.jupiter.api.Test;

import java.io.IOException;

public class producerConsumerTest {
    String topic = "test";
    String partition_count = "18"; //TODO rename partition count???
    String within = "100";
    String window = "10";
    String run_nr = "0";
    String[] args = {topic, partition_count, within, window, run_nr};

    @Test
    void ProducerMain2Test() throws InterruptedException {
        ProducerMain3.main(args);
    }
    @Test
    void CustomResultDumper2Test() throws IOException, RestClientException {
        CustomResultDumper2.main(args);
    }
    @Test
    void KEPLrMain2Test() throws IOException, RestClientException {
        KEPLrMain2.main(args);
    }
}
