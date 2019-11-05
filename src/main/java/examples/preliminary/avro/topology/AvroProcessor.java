package examples.preliminary.avro.topology;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

public class AvroProcessor implements Processor<String, GenericRecord> {

    ProcessorContext context;
    KeyValueStore<String, Double> stateStore;

    @Override
    public void init(ProcessorContext context) {
        this.context=context;
        this.stateStore=(KeyValueStore) context.getStateStore("payments-avgs");
    }

    @Override
    public void process(String key, GenericRecord value) {

        String temp = value.get("id").toString();

        System.out.println(value.get("amount"));

        if(stateStore.get(temp)!=null){
            this.stateStore.put(temp, (Double)(value.get("amount")) + stateStore.get(temp));
        }else{
            this.stateStore.put(temp, (Double)(value.get("amount")));
        }



        stateStore.all().forEachRemaining(
                kv -> {context.forward(kv.key, kv.value);}
        );
    }

    @Override
    public void close() {

    }
}
