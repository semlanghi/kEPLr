package evaluation.esper;

import com.espertech.esper.common.client.EventBean;
import com.espertech.esper.runtime.client.EPRuntime;
import com.espertech.esper.runtime.client.EPStatement;
import com.espertech.esper.runtime.client.UpdateListener;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerListener implements UpdateListener {

    private KafkaProducer<String,GenericRecord> producer;

    private Schema schema;
    private GenericRecordBuilder builder;
    private String topic;

    public ProducerListener(KafkaProducer<String, GenericRecord> producer, String topic) {
        this.producer = producer;
        schema = SchemaBuilder.record("A_X_B").fields()
                .requiredLong("start_time")
                .requiredLong("end_time")
                .requiredLong("idA")
                .requiredLong("idB")
                .endRecord();
        this.topic = topic;
        builder = new GenericRecordBuilder(schema);
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents, EPStatement statement, EPRuntime runtime) {
        for (EventBean bean : newEvents
             ) {
            producer.send(new ProducerRecord<String, GenericRecord>(topic, (String) bean.get("key"),builder
                    .set("start_time", bean.get("x.start_time"))
                    .set("end_time", bean.get("y.end_time"))
                    .set("idA", bean.get("x.idA"))
                    .set("idB", bean.get("y.idB"))
                    .build()));


        }
    }
}
