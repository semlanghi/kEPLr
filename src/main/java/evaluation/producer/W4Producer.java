package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class W4Producer extends WProducerBase {
    private static long ID = 0;

    /**
     * Creates sequential records of single type A and n type B in fixed "time chunks"
     */
    public static void main(String[] args) throws IOException, RestClientException {
        setup(args);
        createRecords();
    }

    private static void createRecords() {
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS-1; i++) {
            long simulatedTime = 1 + i * WITHIN;
            int currentChunkSize = INITIAL_CHUNK_SIZE - 1 + GROWTH_SIZE * i;
            createSequentialAnB(currentChunkSize, simulatedTime);
            System.out.println("Created chunk number: " + (i + 1));
        }
        //sendEndRecord(ID);
        int i = NUMBER_OF_CHUNKS -1;
        long simulatedTime = 1 + i * WITHIN;
        int currentChunkSize = INITIAL_CHUNK_SIZE - 1 + GROWTH_SIZE * i;
        createLastSequentialAnB(currentChunkSize, simulatedTime);

    }

    private static void createSequentialAnB(int n, long time) {
        createRecordA(ID++, time,false);
        for (int i = 1; i <= n; i++) {
            createRecordB(ID++, time + i,false);
        }
    }

    private static void createLastSequentialAnB(int n, long time) {
        createRecordA(ID++, time,true);
        for (int i = 1; i <= n-1; i++) {
            createRecordB(ID++, time + i,false);
        }
        int i = n;
        createRecordB(ID++, time + i,true);
    }
}