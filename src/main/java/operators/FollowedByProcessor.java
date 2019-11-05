package operators;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.commons.lang3.SerializationException;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.HashMap;

public class FollowedByProcessor implements Processor<String, GenericRecord> {



    private ProcessorContext context;
    private KeyValueStore<GenericRecord, Integer> stateStore;
    private String predecessor;
    private String successor;
    private Schema resultSchema;
    private boolean schemaSet=false;
    private GenericRecordBuilder nwEventsBuilder;
    private int actual;
    private HashMap<GenericRecord, Integer> store;


    public FollowedByProcessor(String predecessor, String successor) {
        this.predecessor = predecessor;
        this.successor = successor;
        this.store = new HashMap<>();

    }

    @Override
    public void init(ProcessorContext context) {
        this.context=context;
        this.stateStore=(KeyValueStore) context.getStateStore("events_following");
        this.actual=0;
    }

    @Override
    public void process(String key, GenericRecord value) {


        if(value.getSchema().getName()==predecessor)
            store.put(value, actual);

        // First event arrives, first part of the schema
        if(resultSchema==null && context.topic().equals(predecessor)){
            resultSchema = value.getSchema();
            System.out.println("First presdecessor arrived, first part of the schema set");
        }else
            // Second event arrives, second part of the schema, composition
            if(!schemaSet && context.topic().equals(successor)){
                System.out.println("First successor arrived, second part of the schema set");
                //resultSchema.getFields().addAll(value.getSchema().getFields());

                // Composition of the new schema, dinamically generated
                resultSchema = SchemaBuilder.record("Composed").fields().name("event_1").type(resultSchema).noDefault().name("event_2").type(value.getSchema()).noDefault().endRecord();

                nwEventsBuilder = new GenericRecordBuilder(resultSchema);
                schemaSet=true;
        }


        if(schemaSet && context.topic().equals(successor)) {
            System.out.println("Event Generated");
            /*for (Schema.Field field : value.getSchema().getFields()
            ) {
                nwEventsBuilder.set(field, value.get(field.name()));
            }*/
            nwEventsBuilder.set("event_2", value);
            for(GenericRecord kv: store.keySet())
                    {
                        if(schemaSet && kv!=null){
                            nwEventsBuilder.set("event_1", kv);

                            context.forward(key,nwEventsBuilder.build());
                        }

                    }
        }

        actual++;


    }

    @Override
    public void close() {

    }
}
