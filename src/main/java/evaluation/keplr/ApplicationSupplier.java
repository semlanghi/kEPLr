package evaluation.keplr;

import org.apache.kafka.streams.KafkaStreams;

/**
 * Class that contains the reference to the KafkaStreams object.
 * It sets up the closure of all the application instances from the
 * ThroughputProcessor.
 *
 * @see org.apache.kafka.streams.keplr.operators.ThroughputSupplier
 * @see KafkaStreams
 */
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

    /**
     * Increments the count of terminated instances (end event arrived). Once
     * all are terminated, close the application.
     *
     * @see KafkaStreams#close()
     */
    public void close() {
        if (closed >= tasks)
            app.close();
        else
            closed++;

    }
}
