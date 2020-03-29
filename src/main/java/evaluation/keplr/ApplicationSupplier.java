package evaluation.keplr;

import org.apache.kafka.streams.KafkaStreams;

public class ApplicationSupplier {

    private final int tasks;
    private int closed = 0;

    KafkaStreams app;

    public ApplicationSupplier(int tasks) {
        this.tasks = tasks;
    }

    public KafkaStreams getApp() {
        return app;
    }

    public void setApp(KafkaStreams app) {
        this.app = app;
    }

    public void close() {
        if (closed >= tasks)
            app.close();
        else
            closed++;

    }
}
