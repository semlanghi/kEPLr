package operators;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class AsProcessor implements Processor<String, GenericRecord> {

    ProcessorContext context;
    private String varName;
    GenericRecordBuilder builder;
    Schema schema;

    public AsProcessor(String varName) {
        this.varName = varName;
    }

    @Override
    public void init(ProcessorContext context) {
        this.context=context;
    }

    @Override
    public void process(String key, GenericRecord value) {
        //create new schema, with x.attribute in place of attribute, for each field of the value

        if(schema==null){
            schema = SchemaBuilder.record(value.getSchema().getName() + "_as_" + varName).fields().name(varName).type(value.getSchema()).noDefault().endRecord();
            builder=new GenericRecordBuilder(schema);
        }

        builder.set(varName, value);
        context.forward(key,builder.build());



    }

    @Override
    public void close() {

    }
}
