package operators;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class FirstProcessor  implements Processor<String,GenericRecord> {

    private ProcessorContext context;
    private boolean first= false;

    @Override
    public void init(ProcessorContext context) {
        this.context=context;

    }

    @Override
    public void process(String key, GenericRecord value) {
        if (!first){
            context.forward(key, value);
            first=false;
        }
    }

    @Override
    public void close() {

    }
}
