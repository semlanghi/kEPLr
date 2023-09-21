package utils;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.logging.Logger;

/**
 * Simple Listener for dumping the output of a specific query.
 */
public class LoggingListener implements UpdateListener {

    private final Logger LOGGER = Logger.getLogger(UpdateListener.class.getName());
    private FileWriter writer;

    public LoggingListener(File file) {
        try {
            this.writer = new FileWriter(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        try {
            writer.write(newEvents[0].getUnderlying().toString()+"\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
