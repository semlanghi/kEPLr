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
        setup(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        setNumberOfChunks();
        createRecords();
    }

    private static void createRecords() {
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS; i++) {
            for (int j = 0; j < Math.pow(2, 1+i); j+=2) {
                long simulatedTime = 1 + i* CHUNK_SIZE + j;
                createSequentialAB(simulatedTime);
            }
            System.out.println("Created chunk number: " + (i + 1));
        }
        sendEndRecord(ID);
    }

    private static void createSequentialAB(long time){
        createRecordA(ID, time);
        createRecordB(ID++, time + 1);
    }


    private static void setNumberOfChunks(){
        NUMBER_OF_CHUNKS = (int) Math.floor( Math.log(CHUNK_SIZE) / Math.log(2));
    }
}

