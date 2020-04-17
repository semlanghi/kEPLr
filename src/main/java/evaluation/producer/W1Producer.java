package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

/**
 * Creates records for example 1.
 * The records are grouped into chunks of size equal to the
 * {@link evaluation.keplr.WBase#within} parameter. The records are set up as in the following:
 *
 * <-------------within---------------><-------------within--------------->
 * <-----As-----><-----Bs----->        <-----As-----><-----Bs----->
 * AAAAAAAAAAA...BBBBBBBBBBBB...       AAAAAAAAAAA...BBBBBBBBBBBB...
 *
 * @see evaluation.keplr.W1
 */
public class W1Producer extends WProducerBase{
    private static long ID = 0;

    /**
     * Creates sequential n type A and n type B records in fixed "time chunks"
     */
    public static void main(String[] args) throws IOException, RestClientException{
        setup(args);
        createRecords();
    }

    private static void createRecords(){
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS-1; i++) {
            long simulatedTime = 50 + i * WITHIN + INITIAL_SIMULATED_TIME;
            int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;
            createSequentialnAnB(currentChunkSize/2, simulatedTime);
            System.out.println("Created chunk number: " + (i + 1) + " for partition "+PARTITION_ASSIGNED);
        }
        int i = NUMBER_OF_CHUNKS-1;
        long simulatedTime = 50 + i * WITHIN + INITIAL_SIMULATED_TIME;
        int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;

        if(PARTITIONS==3){
            if(PARTITION_ASSIGNED>=6)
                createLastSequentialnAnB(currentChunkSize/2, simulatedTime);
            else createSequentialnAnB(currentChunkSize/2, simulatedTime);
        } else if(PARTITIONS==6){
            if(PARTITION_ASSIGNED>=3)
                createLastSequentialnAnB(currentChunkSize/2, simulatedTime);
            else createSequentialnAnB(currentChunkSize/2, simulatedTime);
        } else if(PARTITIONS==9){
            createLastSequentialnAnB(currentChunkSize/2, simulatedTime);
        }

    }

    private static void createSequentialnAnB(int n, long time){
        for (int i = 0; i < n; i++) {
            createRecordA(ID++, time + i, false);
        }

        for (int i = 0; i < n; i++) {
            createRecordB(ID++, time + i + n, false);
        }
    }

    private static void createLastSequentialnAnB(int n, long time){
        for (int i = 0; i < n-1; i++) {
            createRecordA(ID++, time + i, false);
        }
        createRecordA(ID++, time+(n-1), true);
        for (int i = 0; i < n-1; i++) {
            createRecordB(ID++, time + i + n, false);
        }
        createRecordB(ID++, time + n - 1 + n, true);
    }
}