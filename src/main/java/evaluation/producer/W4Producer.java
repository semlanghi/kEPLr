package evaluation.producer;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

import java.io.IOException;
import java.util.Properties;

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

    public W4Producer(Properties properties) {
        super(properties);
    }

    @Override
    protected void createKeyedLastBatch(int currentChunkSize, int key) {
        sendRecordA(ID++, simulatedTime, false, key);

        int i;
        for (i = 1; i <= currentChunkSize-1; i++) {
            sendRecordB(ID++, simulatedTime + i, false, key);
        }
        sendRecordEND(ID++, simulatedTime + i, key);
    }

    @Override
    protected void createKeyedBatch(int currentChunkSize, int key) {
        sendRecordA(ID++, simulatedTime,false, key);

        for (int i = 1; i < currentChunkSize; i++) {
            sendRecordB(ID++, simulatedTime + i,false, key);
        }
    }
}