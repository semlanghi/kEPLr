package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

public class W4Producer extends WProducerBase{
    private static long ID = 0;

    /**
     * Creates sequential records of single type A and n type B in fixed "time chunks"
     */
    public static void main(String[] args) throws IOException, RestClientException {
        setup(args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        setNumberOfChunks();
        createRecords();
    }

    private static void createRecords(){
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS; i++) {
            long simulatedTime = 1 + i* CHUNK_SIZE;
            createSequentialAnB((int) Math.pow(2, i+1), simulatedTime);
            System.out.println("Created chunk number: " + (i + 1));
        }
    }

    private static void createSequentialAnB(int n, long time){
        createRecordA(ID++, time);
        for (int i = 1; i <= n; i++) {
            createRecordB(ID++, time + i);
        }
    }

    private static void setNumberOfChunks() {
        NUMBER_OF_CHUNKS = (int) Math.floor( Math.log(CHUNK_SIZE-1) / Math.log(2));
    }
}