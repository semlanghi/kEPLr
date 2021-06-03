package utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * A {@link TimestampExtractor} for the {@link String} representation of the value.
 * It extracts the timstamp from the last section of the string value, with respect to the
 * period-based (".") splitting.
 */
public class MyTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if(((String)record.value()).contains(".")){
            String[] chunked = ((String)record.value()).split("\\.");
            return Long.parseLong(chunked[chunked.length-1]);
        } else return 0L;
    }
}
