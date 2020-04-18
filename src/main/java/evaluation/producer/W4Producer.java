package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

/**
 * Creates records for example 1.
 * The records are grouped into chunks of size equal to the
 * {@link evaluation.keplr.WBase#within} parameter. The records are set up as in the following:
 *
 * <-------------within---------------><-------------within--------------->
 *  <-----Bs----->                      <-----Bs----->
 * ABBBBBBBBBBBB...                    ABBBBBBBBBBBB...
 *
 * @see evaluation.keplr.W4
 */
public class W4Producer extends WProducerBase {
    private static long ID = 0;

    public static void main(String[] args) throws IOException, RestClientException {
        setup(args);
        createRecords();
    }

    private static void createRecords() {
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS-1; i++) {
            long simulatedTime = 1 + i * WITHIN + INITIAL_SIMULATED_TIME;
            int currentChunkSize = INITIAL_CHUNK_SIZE - 1 + GROWTH_SIZE * i;
            createSequentialAnB(currentChunkSize, simulatedTime);
            System.out.println("Created chunk number: " + (i + 1));
        }
        //sendEndRecord(ID);
        int i = NUMBER_OF_CHUNKS -1;
        long simulatedTime = 1 + i * WITHIN + INITIAL_SIMULATED_TIME;
        int currentChunkSize = INITIAL_CHUNK_SIZE - 1 + GROWTH_SIZE * i;


        if(PARTITIONS==1){
            createLastSequentialAnB(currentChunkSize, simulatedTime);

        }else if(PARTITIONS==3){
            if(PARTITION_ASSIGNED>=6)
                createLastSequentialAnB(currentChunkSize, simulatedTime);
            else createSequentialAnB(currentChunkSize, simulatedTime);
        } else if(PARTITIONS==6){
            if(PARTITION_ASSIGNED>=3)
                createLastSequentialAnB(currentChunkSize, simulatedTime);
            else createSequentialAnB(currentChunkSize, simulatedTime);
        } else if(PARTITIONS==9){
            createLastSequentialAnB(currentChunkSize, simulatedTime);
        }


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