package evaluation.esper;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import com.opencsv.CSVWriter;
import utils.PerformanceFileBuilder;

import java.io.FileWriter;
import java.io.IOException;

public class DumpingListener implements UpdateListener {

    private final CSVWriter outputDumpWriter;
    private final boolean dump;
    private final Runtime runtime;
    private final long within;
    private PerformanceFileBuilder performanceFileBuilder;
    private PerformanceFileBuilder memoryFileBuilder;
    private long counter =0L;
    private String exp;
    private String run;
    private long startTime = System.currentTimeMillis();
    private boolean ended = false;




    public DumpingListener(String expName, boolean dump, long within) throws IOException {
        String[] split = expName.split("_");
        this.exp = split[0];
        this.run = split[1];
        this.within = within;
        String[] headersThroughput = {"exp-name", "run", "thread", "within", "start-time", "end-time", "nevents"};
        String[] headersMemory = {"exp-name", "run", "within", "time", "total-memory", "free-memory",  "nevents"};
        this.runtime = Runtime.getRuntime();
        this.performanceFileBuilder = new PerformanceFileBuilder(expName + "_throughput_esper.csv", headersThroughput,true,
                expName + "_memory_esper_" + Thread.currentThread().getName() + ".csv", headersMemory,true);
        this.dump = dump;
        if (dump) {
            outputDumpWriter = new CSVWriter(new FileWriter(expName + "esper-output-dump.csv", true));
            String[] header = {"AXB", "start_time", "start_time", "end_time", "end", "idA", "start_timeA", "end_timeA", "partitionA", "isEndA",
                    "idB", "start_timeB", "end_timeB", "partitionB", "isEndB"};
            outputDumpWriter.writeNext(header, false);
            outputDumpWriter.flush();
        } else outputDumpWriter = null;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        try {
            for (EventBean bean : newEvents) {
                counter++;
                if (dump) {
                    String[] nextLine = {"AXB",
                            String.valueOf(bean.get("x.start_time")),
                            String.valueOf(bean.get("y.end_time")),
                            String.valueOf((boolean) bean.get("x.end") && (boolean) bean.get("y.end")),
                            String.valueOf(bean.get("x.idA")),
                            String.valueOf(bean.get("x.start_time")),
                            String.valueOf(bean.get("x.end_time")),
                            String.valueOf(bean.get("x.partition")),
                            String.valueOf(bean.get("x.end")),
                            String.valueOf(bean.get("y.idB")),
                            String.valueOf(bean.get("y.start_time")),
                            String.valueOf(bean.get("y.end_time")),
                            String.valueOf(bean.get("y.partition")),
                            String.valueOf(bean.get("y.end"))};


                    outputDumpWriter.writeNext(nextLine, false);
                    outputDumpWriter.flush();
                }

//                System.out.println(bean.get("x.end_time"));
                if ((boolean) bean.get("x.end") && (boolean) bean.get("y.end")){
                    String[] thData = new String[]{
                            this.exp,
                            this.run,
                            Thread.currentThread().getName(),
                            String.valueOf(within),
                            String.valueOf(startTime),
                            String.valueOf(System.currentTimeMillis()),
                            String.valueOf(counter)
                    };
                    performanceFileBuilder.registerThroughput(thData);
                    ended = true;
                }
            }
            String[] memData = new String[]{
                    this.exp,
                    this.run,
                    String.valueOf(this.within),
                    String.valueOf(this.runtime.totalMemory()),
                    String.valueOf(this.runtime.freeMemory()),
                    String.valueOf(System.currentTimeMillis()),
                    String.valueOf(counter)
            };
            performanceFileBuilder.registerMemory(memData);

            if (ended){
                if (dump)
                    outputDumpWriter.close();
                performanceFileBuilder.close();
            }




        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean getEnd() {
        return ended;
    }
}
