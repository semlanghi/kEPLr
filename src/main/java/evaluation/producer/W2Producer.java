package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Properties;

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

    public W2Producer(Properties properties) {
        super(properties);
    }


    @Override
    protected void createKeyedLastBatch(int currentChunkSize, int key) {
        int i;
        for (i = 0; i < currentChunkSize-2; i++) {
            sendRecordA(ID++, simulatedTime + i, false, key);
        }
        sendRecordB(ID++, simulatedTime + i, false, key);
        sendRecordEND(ID++, simulatedTime + currentChunkSize - 1, key);
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
