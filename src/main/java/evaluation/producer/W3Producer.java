package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class W3Producer extends WProducerBase{

    private static long ID = 0;
    /**
     * Creates sequential records (pairs) of type A and type B in fixed "time chunks"
     * Each pair contains of:
     * record A with idA: X and start_time=end_time=Y
     * record B with idB: X and start_time=end_time=Y+1
     * Every new chunk has double the size (number of pairs) of previous chunk
     */

    public static void main(String[] args) throws IOException, RestClientException {
        setup(args);
        createRecords();
    }

    private static void createRecords() {
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS-1; i++) {
            long simulatedTime = 1 + i* WITHIN;
            int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;
            createSequentialAB(currentChunkSize/2, simulatedTime);

            System.out.println("Created chunk number: " + (i + 1)+" for partition " + PARTITION_ASSIGNED);
        }
        int i = NUMBER_OF_CHUNKS-1;
        long simulatedTime = 1 + i* WITHIN;
        int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;
        createLastSequentialAB(currentChunkSize/2, simulatedTime);
        //sendEndRecord(ID);
    }

    private static void createSequentialAB(int nrOfPairs, long time){
        for (int j = 0; j < nrOfPairs; j++) {
            createRecordA(ID, time + j* 2, false);
            createRecordB(ID++, time + j*2 + 1, false);
        }
    }
    private static void createLastSequentialAB(int nrOfPairs, long time){
        for (int j = 0; j < nrOfPairs-1; j++) {
            createRecordA(ID, time + j* 2, false);
            createRecordB(ID++, time + j*2 + 1, false);
        }
        int j = nrOfPairs - 1;
        createRecordA(ID, time + j* 2, true);
        createRecordB(ID++, time + j*2 + 1, true);
    }
}

