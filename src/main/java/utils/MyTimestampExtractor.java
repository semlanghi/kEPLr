package utils;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

public class MyTimestampExtractor implements TimestampExtractor {


    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {

        if(((String)record.value()).contains(".")){
            String[] chunked = ((String)record.value()).split("\\.");
            return Long.parseLong(chunked[chunked.length-1]);
        } else return 0L;
    }
}
