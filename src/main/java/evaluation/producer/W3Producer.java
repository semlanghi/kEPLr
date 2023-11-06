package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Properties;

/**
 * Creates records for example 3.
 * The records are grouped into chunks of size equal to the
 *  parameter. The records are set up as in the following:
 *
 * <-------------within---------------><-------------within--------------->
 * ABABABABABAB...                     ABABABABABABAB...
 *
 * @see evaluation.keplr.W3
 */
public class W3Producer extends WProducerBase{

    public W3Producer(Properties properties) {
        super(properties);
    }

    @Override
    protected void createKeyedLastBatch(int currentChunkSize, int key) {
        int nrOfPairs = currentChunkSize/2;

        int j;
        for (j = 0; j < nrOfPairs-1; j++) {
            sendRecordA(ID++, simulatedTime + j * 2, false, key);
            sendRecordB(ID++, simulatedTime + j * 2 + 1, false, key);
        }
        sendRecordA(ID++, simulatedTime + j * 2, true, key);
        sendRecordB(ID++, simulatedTime + j * 2 + 1, true, key);
        sendRecordEND(ID++, simulatedTime + j*2 + 2, key);
    }

    @Override
    protected void createKeyedBatch(int currentChunkSize, int key) {
        int nrOfPairs = currentChunkSize/2;

        for (int j = 0; j < nrOfPairs; j++) {
            sendRecordA(ID++, simulatedTime + j * 2, false, key);
            sendRecordB(ID++, simulatedTime + j * 2 + 1, false, key);
        }
    }
}

