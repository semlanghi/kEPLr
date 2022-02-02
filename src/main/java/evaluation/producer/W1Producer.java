package evaluation.producer;

import lombok.extern.log4j.Log4j;

import java.util.Properties;

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

@Log4j
public class W1Producer extends WProducerBase{


    public W1Producer(Properties properties) {
        super(properties);
    }


    @Override
    protected void createKeyedBatch(int chunkSize, int key){
        int numberOfAsAndBs = chunkSize/2;

        for (int i = 0; i < numberOfAsAndBs; i++) {
            sendRecordA(ID++, simulatedTime + i, false, key);
        }
        for (int i = 0; i < numberOfAsAndBs; i++) {
            sendRecordB(ID++, simulatedTime + i + numberOfAsAndBs, false, key);
        }
    }

    @Override
    protected void createKeyedLastBatch(int chunckSize, int key){
        int numberOfAsAndBs = chunckSize/2;
        for (int i = 0; i < numberOfAsAndBs-1; i++) {
            sendRecordA(ID++, simulatedTime + i, false,key);
        }
        for (int i = 0; i < numberOfAsAndBs-1; i++) {
            sendRecordB(ID++, simulatedTime + i + chunckSize, false,key);
        }
        sendRecordEND(ID++, simulatedTime + chunckSize - 1 + chunckSize, key);
    }
}