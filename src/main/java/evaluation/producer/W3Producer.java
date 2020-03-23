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
        TOPIC = args[0];
        PARTITIONS = Integer.parseInt(args[1]);
        INITIAL_CHUNK_SIZE = Integer.parseInt(args[2]);
        NUMBER_OF_CHUNKS = Integer.parseInt(args[3]);
        GROWTH_SIZE = Integer.parseInt(args[4]);
        MAX_CHUNK_SIZE = INITIAL_CHUNK_SIZE + GROWTH_SIZE * NUMBER_OF_CHUNKS;

        setup();
        createRecords();
    }

    private static void createRecords() {
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS; i++) {
            long simulatedTime = 1 + i* MAX_CHUNK_SIZE;
            int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;
            createSequentialAB(currentChunkSize/2, simulatedTime);

            System.out.println("Created chunk number: " + (i + 1));
        }
        sendEndRecord(ID);
    }

    private static void createSequentialAB(int nrOfPairs, long time){
        for (int j = 0; j < nrOfPairs; j++) {
            createRecordA(ID, time + j* 2);
            createRecordB(ID++, time + j*2 + 1);
        }
    }
}

