package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

/**
 * Creates records for example 2.
 * The records are grouped into chunks of size equal to the
 * {@link evaluation.keplr.WBase#within} parameter. The records are set up as in the following:
 *
 * <-------------within---------------><-------------within--------------->
 * <-----As----->                      <-----As----->
 * AAAAAAAAAAA...B                     AAAAAAAAAAA...B
 *
 * @see evaluation.keplr.W2
 */
public class W2Producer extends WProducerBase {
    private static long ID = 1;

    public static void main(String[] args) throws IOException, RestClientException {


        setup(args);
        createRecords();
    }

    private static void createRecords() {
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS - 1; i++) {
            long simulatedTime = 1 + i * WITHIN+INITIAL_SIMULATED_TIME;
            int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;
            createSequentialnAB(currentChunkSize - 1, simulatedTime);
            System.out.println("Created chunk number: " + (i + 1)+ " for part "+PARTITION_ASSIGNED);
        }
        long simulatedTime = 1 + (NUMBER_OF_CHUNKS-1) * WITHIN +INITIAL_SIMULATED_TIME;
        int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * (NUMBER_OF_CHUNKS-1);

        if(PARTITIONS==3){
            if(PARTITION_ASSIGNED>=6)
                createLastSequentialnAB(currentChunkSize - 1, simulatedTime);
            else createSequentialnAB(currentChunkSize - 1, simulatedTime);
        } else if(PARTITIONS==6){
            if(PARTITION_ASSIGNED>=3)
                createLastSequentialnAB(currentChunkSize - 1, simulatedTime);
            else createSequentialnAB(currentChunkSize - 1, simulatedTime);
        } else if(PARTITIONS==9){
            createLastSequentialnAB(currentChunkSize - 1, simulatedTime);
        }
    }

    private static void createSequentialnAB(int n, long time) {
        for (int i = 0; i < n; i++) {
            createRecordA(ID++, time + i, false);
        }
        createRecordB(ID++, time + n, false);
    }

    private static void createLastSequentialnAB(int n, long time) {
        for (int i = 0; i < n-1; i++) {
            createRecordA(ID++, time + i, false);
        }
        createRecordA(ID++, time + n - 1, true);
        createRecordB(ID++, time + n, true);
    }
}
