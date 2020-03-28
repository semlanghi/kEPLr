package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class W2Producer extends WProducerBase {
    private static long ID = 1;

    /**
     * Creates sequential n type A and a single type B record in fixed "time chunks"
     */
    public static void main(String[] args) throws IOException, RestClientException {


        setup(args);
        createRecords();
    }

    private static void createRecords() {
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS; i++) {
            long simulatedTime = 1 + i * WITHIN;
            int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;
            createSequentialnAB(currentChunkSize - 1, simulatedTime);
            System.out.println("Created chunk number: " + (i + 1));
        }
        sendEndRecord(ID);
    }

    private static void createSequentialnAB(int n, long time) {
        for (int i = 0; i < n; i++) {
            createRecordA(ID++, time + i);
        }
        createRecordB(ID++, time + n);
    }
}
