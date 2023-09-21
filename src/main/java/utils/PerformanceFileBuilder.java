package utils;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class PerformanceFileBuilder {


    private String platform;
    private String fileName;

    public PerformanceFileBuilder(String fileName) {
        try {
            this.fileName = fileName;
            File file = new File(fileName);
            if(!file.exists()){
                file.createNewFile();
                CSVWriter writer = new CSVWriter(new FileWriter(file, true));
                String[] firstRow = new String[]{"Experiment-Name", "Experiment-Run", "Platform", "Throughput", "InputSize", "SecondsPassed"};
                writer.writeNext(firstRow);
                writer.flush();
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void register(String expName, String expRun, double throughput, long inputSize, long secondsPassed){
        String[] row = new String[]{expName, expRun, String.valueOf(throughput), String.valueOf(inputSize), String.valueOf(secondsPassed)};
        File file = new File(fileName);

        try {
            CSVWriter writer = new CSVWriter(new FileWriter(file, true));
            writer.writeNext(row);
            writer.flush();
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
