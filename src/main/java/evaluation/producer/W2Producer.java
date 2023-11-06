package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Properties;

/**
 * Creates records for example 2.
 * The records are grouped into chunks of size equal to the
 *  parameter. The records are set up as in the following:
 *
 * <-------------within---------------><-------------within--------------->
 * <-----As----->                      <-----As----->
 * AAAAAAAAAAA...B                     AAAAAAAAAAA...B
 *
 * @see evaluation.keplr.W2
 */
public class W2Producer extends WProducerBase {

    public W2Producer(Properties properties) {
        super(properties);
    }


    @Override
    protected void createKeyedLastBatch(int currentChunkSize, int key) {
        int i;
        for (i = 0; i < currentChunkSize-3; i++) {
            sendRecordA(ID++, simulatedTime + i, false, key);
        }
        sendRecordA(ID++, simulatedTime + i++, true, key);
        sendRecordB(ID++, simulatedTime + i++, true, key);
        sendRecordEND(ID++, simulatedTime + i, key);
    }

    @Override
    protected void createKeyedBatch(int currentChunkSize, int key) {
        int i;
        for (i = 0; i < currentChunkSize-1; i++) {
            sendRecordA(ID++, simulatedTime + i, false, key);
        }
        sendRecordB(ID++, simulatedTime + i, false, key);
    }


}
