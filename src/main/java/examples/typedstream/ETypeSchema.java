package examples.typedstream;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.kstream.KStream;

public class ETypeSchema extends EType<GenericRecord, Schema>  {

    public ETypeSchema(Schema type) {
        super(type);
    }

    @Override
    public boolean check(GenericRecord event) {
        return event.getSchema().getName().equals(type);
    }
}
