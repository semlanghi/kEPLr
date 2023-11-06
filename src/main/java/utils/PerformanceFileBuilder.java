package utils;

import com.opencsv.CSVWriter;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class PerformanceFileBuilder {

    private CSVWriter throughputWriter;
    private CSVWriter memoryWriter;

    public PerformanceFileBuilder(String throughputFileName, String[] throughputHeaders, boolean thAppend,
                                  String memoryFileName, String[] memoryHeaders, boolean memAppend) {
        try {
            File file = new File(throughputFileName);
            if(!file.exists()){
                file.createNewFile();
                this.throughputWriter = new CSVWriter(new FileWriter(file, thAppend));
                this.throughputWriter.writeNext(throughputHeaders);
                this.throughputWriter.flush();
            } else this.throughputWriter = new CSVWriter(new FileWriter(file, thAppend));
            file = new File(memoryFileName);
            if(!file.exists()){
                file.createNewFile();
                this.memoryWriter = new CSVWriter(new FileWriter(file, memAppend));
                this.memoryWriter.writeNext(memoryHeaders);
                this.memoryWriter.flush();
            } else this.memoryWriter = new CSVWriter(new FileWriter(file, memAppend));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void registerThroughput(String[] row){
        try {
            throughputWriter.writeNext(row);
            throughputWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void registerMemory(String[] row){
        try {
            memoryWriter.writeNext(row);
            memoryWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() throws IOException {
        this.throughputWriter.close();
        this.memoryWriter.close();
    }
}
