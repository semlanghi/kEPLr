package org.apache.kafka.streams.keplr.etype;

import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.streams.kstream.ValueJoiner;

import java.util.ArrayList;
import java.util.Objects;
import java.util.function.ToLongFunction;

public class ETypeAvro extends EType<String, GenericRecord> {

    public Schema getSchema() {
        return schema.getAvroSchema();
    }

    //@JsonSerialize(using = AvroModule.SchemaSerializer.class)
    private AvroSchema schema;


    //private GenericRecordBuilder recordBuilder;

    public ETypeAvro() {
    }



    public ETypeAvro(Schema schema) {
        this.description = schema.getName();

        this.schema = new AvroSchema(schema);

        //this.recordBuilder = new GenericRecordBuilder(schema);
    }

    @Override
    public EType<String, GenericRecord> everyVersion() {
        EType<String,GenericRecord> type = new ETypeAvro(schema.getAvroSchema());
        type.setOnEvery(true);
        type.chunk(this.isChunkLeft(), this.isChunkRight());
        return type;
    }

    @Override
    public boolean isThisTheEnd(GenericRecord value) {
        return (boolean) value.get("end");
    }

    @Override
    public TypedKey<String> typed(String key) {
        return new TypedKey<>(key,this.description);
    }

    @Override
    public String untyped(TypedKey<String> typedKey) {
        return typedKey.getKey();
    }

    @Override
    public long start(GenericRecord value) {
        return (long) value.get(schema.getAvroSchema().getField("start_time").name());

    }

    @Override
    public long end(GenericRecord value) {
        return (long) value.get(schema.getAvroSchema().getField("end_time").name());
    }



    @Override
    public EType<String, GenericRecord> product(EType<String, GenericRecord> otherType, boolean array) {

         if(otherType instanceof ETypeAvro) {
             Schema schema = SchemaBuilder.record(this.description + "_X_" + otherType.description).fields()
                     .requiredLong("start_time")
                     .requiredLong("end_time")
                     .requiredBoolean("end")
                     .name("x")
                     .type(this.schema.getAvroSchema())
                     .noDefault()
                     .name("y")
                     .type(((ETypeAvro) otherType).schema.getAvroSchema())
                     .noDefault()
                     .endRecord();
             return new ETypeAvro(schema);
         }else {
             System.out.println("product not possible between incompatible types");
             return null;
         }


    }

    @Override
    public ValueJoiner<GenericRecord, GenericRecord, GenericRecord> joiner() {
        if(this.schema.getAvroSchema().getField("x")!=null){
            return new ValueJoiner<GenericRecord, GenericRecord, GenericRecord>() {
                GenericRecordBuilder recordBuilder=new GenericRecordBuilder(schema.getAvroSchema());
                @Override
                public GenericRecord apply(GenericRecord value1, GenericRecord value2) {

                    return recordBuilder.set("x", value1).set("y",value2).set("start_time", (long)value1.get("start_time"))
                            .set("end_time", (long)value2.get("end_time"))
                            .set("end", ((boolean) value1.get("end")) && ((boolean) value2.get("end"))).build();
                }
            };
        }else{
            System.out.println("This is not a composite type, so no joiner can be get from it.");
            return null;
        }

    }

    @Override
    public Class<String> kClass() {
        return String.class;
    }

    @Override
    public ArrayList<GenericRecord> extract(GenericRecord value) {
        ArrayList<GenericRecord> records = new ArrayList<>();
        records.add((GenericRecord) value.get("x"));
        records.add((GenericRecord) value.get("y"));
        return records;
    }

    @Override
    public GenericRecord wrap(ArrayList<GenericRecord> value) {

        if(value.size()<=1){
            return value.get(0);
        }

        if(schema.getAvroSchema().getField("x")!=null){


            long minStart = value.stream().mapToLong(new ToLongFunction<GenericRecord>() {
                @Override
                public long applyAsLong(GenericRecord value) {
                    return (long) value.get("start_time");
                }
            }).min().getAsLong();

            long maxEnd = value.stream().mapToLong(new ToLongFunction<GenericRecord>() {
                @Override
                public long applyAsLong(GenericRecord value) {
                    return (long) value.get("end_time");
                }
            }).max().getAsLong();

            GenericRecordBuilder recordBuilder = new GenericRecordBuilder(schema.getAvroSchema());
            return recordBuilder.set("start_time", minStart)
                    .set("end_time",maxEnd)
                    .set("x", value.get(0))
                    .set("y", value.get(0)).build();
        }else {
            System.out.println("not wrappable");
            return null;
        }
    }


    @Override
    public boolean test(String key, GenericRecord value) {
        return value.getSchema().equals(schema.getAvroSchema());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ETypeAvro)) return false;
        if (!super.equals(o)) return false;
        ETypeAvro eTypeAvro = (ETypeAvro) o;
        return Objects.equals(schema, eTypeAvro.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), schema);
    }
}
