package utils;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * A {@link TimestampExtractor} for the {@link GenericRecord} (Avro) representation.
 * It extracts the timestamp from the "end_time" field of the record.
 *
 * @see GenericRecord
 * @see org.apache.avro.Schema
 */
public class AvroTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        GenericRecord genRecord = (GenericRecord)record.value();
        if(genRecord.get("end_time")!=null){
            return (long) genRecord.get("end_time");
        } else return 0L;
    }
}
