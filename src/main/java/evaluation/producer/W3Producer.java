package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;

/**
 * Creates records for example 3.
 * The records are grouped into chunks of size equal to the
 * {@link evaluation.keplr.WBase#within} parameter. The records are set up as in the following:
 *
 * <-------------within---------------><-------------within--------------->
 * ABABABABABAB...                     ABABABABABABAB...
 *
 * @see evaluation.keplr.W3
 */
public class W3Producer extends WProducerBase{

    private static long ID = 0;

    public static void main(String[] args) throws IOException, RestClientException {
        setup(args);
        createRecords();
    }

    private static void createRecords() {
        System.out.println("Total number of chunks: " + NUMBER_OF_CHUNKS);
        for (int i = 0; i < NUMBER_OF_CHUNKS-1; i++) {
            long simulatedTime = 1 + i* WITHIN + INITIAL_SIMULATED_TIME;
            int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;
            createSequentialAB(currentChunkSize/2, simulatedTime);

            System.out.println("Created chunk number: " + (i + 1)+" for partition " + PARTITION_ASSIGNED);
        }
        int i = NUMBER_OF_CHUNKS-1;
        long simulatedTime = 1 + i* WITHIN + INITIAL_SIMULATED_TIME;
        int currentChunkSize = INITIAL_CHUNK_SIZE + GROWTH_SIZE * i;

        //sendEndRecord(ID);
        if(PARTITIONS==1){
            createLastSequentialAB(currentChunkSize/2, simulatedTime);

        }else if(PARTITIONS==3){
            if(PARTITION_ASSIGNED>=6)
                createLastSequentialAB(currentChunkSize/2, simulatedTime);
            else createSequentialAB(currentChunkSize/2, simulatedTime);
        } else if(PARTITIONS==6){
            if(PARTITION_ASSIGNED>=3)
                createLastSequentialAB(currentChunkSize/2, simulatedTime);
            else createSequentialAB(currentChunkSize/2, simulatedTime);
        } else if(PARTITIONS==9){
            createLastSequentialAB(currentChunkSize/2, simulatedTime);
        }



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

