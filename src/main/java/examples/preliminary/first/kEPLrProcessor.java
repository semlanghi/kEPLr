package examples.preliminary.first;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;

public class kEPLrProcessor implements Processor<String, Integer> {


    private ProcessorContext context;
    private KeyValueStore<String, Integer> stateStore;

    public void init(ProcessorContext processorContext) {

        this.context=processorContext;
        this.stateStore=(KeyValueStore) context.getStateStore("payments-avg");
    }



    public void process(String id, Integer record) {

        stateStore.put(id, record);

        stateStore.all().forEachRemaining(
                kv -> {context.forward(kv.key, kv.value);}
        );

    }



    public void close() {
    }

}

