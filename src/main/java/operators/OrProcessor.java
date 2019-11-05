package operators;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class OrProcessor implements Processor<String, GenericRecord> {

    private ProcessorContext context;


    public void init(ProcessorContext context) {
        this.context=context;
    }

    public void process(String key, GenericRecord value) {
        context.forward(key, value);
    }

    public void close() {

    }
}
