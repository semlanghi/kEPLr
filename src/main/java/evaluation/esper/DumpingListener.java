package evaluation.esper;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import com.opencsv.CSVWriter;
import evaluation.ExperimentsConfig;
import keplr.etype.ETypeAvro;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import utils.PerformanceFileBuilder;

import javax.sound.midi.SoundbankResource;
import java.io.FileWriter;
import java.io.IOException;

public class DumpingListener implements UpdateListener {

    private final GenericRecordBuilder builder;
    private final GenericRecordBuilder builderA;
    private final GenericRecordBuilder builderB;
    private PerformanceFileBuilder performanceFileBuilder;
    private long counter =0L;
    private String exp;
    private String run;
    private long startTime = System.currentTimeMillis();
    private long endCount = 0L;


    public DumpingListener(String expName, Schema schemaA , Schema schemaB) throws IOException {
        this.exp = expName.split("_")[0];
        this.run = expName.split("_")[1];
        performanceFileBuilder = new PerformanceFileBuilder(expName + "performance_esper.csv");
        Schema schema = SchemaBuilder.record("A_X_B").fields()
                .requiredLong("start_time")
                .requiredLong("end_time")
                .requiredBoolean("end")
                .name("x")
                .type(schemaA)
                .noDefault()
                .name("y")
                .type(schemaB)
                .noDefault()
                .endRecord();
        builder = new GenericRecordBuilder(schema);
        builderA = new GenericRecordBuilder(schemaA);
        builderB = new GenericRecordBuilder(schemaB);
        CSVWriter outputDumpWriter = new CSVWriter(new FileWriter(  expName + "esper.output.dump.csv", true));
        String[] header = {"AXB", "start_time", "start_time", "end_time", "end", "idA", "start_timeA", "end_timeA", "partitionA", "isEndA",
                "idB", "start_timeB", "end_timeB", "partitionB", "isEndB"};
        outputDumpWriter.writeNext(header, false);
        outputDumpWriter.flush();
        outputDumpWriter.close();
    }

    private void registerPerformance(){
        long diff = System.currentTimeMillis() - startTime;
        double throughput = (double)counter;
        throughput = throughput/diff;
        throughput = throughput * 1000;

        performanceFileBuilder.register(exp, run, throughput,
                counter, diff/1000);
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        boolean endA = false;
        boolean endB = false;
//            CSVWriter outputDumpWriter = new CSVWriter(new FileWriter(  exp+"_"+run+"_" + "esper.output.dump.csv", true));
        for (EventBean bean : newEvents
             ) {

//                GenericRecord x = builderA.set("start_time", bean.get("x.start_time"))
//                        .set("end_time", bean.get("x.end_time"))
//                        .set("end", bean.get("x.end"))
//                        .set("idA", bean.get("x.idA"))
//                        .set("partition", bean.get("x.partition"))
//                        .build();
//
//                GenericRecord y = builderB.set("start_time", bean.get("y.start_time"))
//                        .set("end_time", bean.get("y.end_time"))
//                        .set("end", bean.get("y.end"))
//                        .set("idB", bean.get("y.idB"))
//                        .set("partition", bean.get("y.partition"))
//                        .build();

            if(!endA || !endB){
                endA = (boolean) bean.get("x.end");
                endB = (boolean) bean.get("y.end");
            }

//                String[] nextLine = {"AXB",
//                        String.valueOf(x.get("start_time")),
//                        String.valueOf(y.get("end_time")),
//                        String.valueOf((boolean) bean.get("x.end") && (boolean) bean.get("y.end")),
//                        String.valueOf(x.get("idA")),
//                        String.valueOf(x.get("start_time")),
//                        String.valueOf(x.get("end_time")),
//                        String.valueOf(x.get("partition")),
//                        String.valueOf(x.get("end")),
//                        String.valueOf(y.get("idB")),
//                        String.valueOf(y.get("start_time")),
//                        String.valueOf(y.get("end_time")),
//                        String.valueOf(y.get("partition")),
//                        String.valueOf(y.get("end"))};

//                if(String.valueOf(x.get("idA")).equals("205") && String.valueOf(y.get("idB")).equals("280")){
//                    System.out.println("HERE IS THE PROBLEM");
//                }
//
//                outputDumpWriter.writeNext(nextLine, false);
//                outputDumpWriter.flush();
//                counter++;

        }

//            outputDumpWriter.close();


        if (endA && endB){
            registerPerformance();
            endCount++;
        }
    }

    public long getEndCount() {
        return endCount;
    }
}
