package utils;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * A {@link Serde} for the Avro Representation, containing the already implemented
 * serializer and deserializer.
 * @see KafkaAvroDeserializer
 * @see KafkaAvroSerializer
 */
public class KafkaAvroSerDe  implements Serde<Object> {

    Serializer<Object> serializer;
    Deserializer<Object> deserializer;

    public KafkaAvroSerDe() {
        this.serializer=new KafkaAvroSerializer();
        this.deserializer=new KafkaAvroDeserializer();
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        serializer.configure(configs, isKey);
        deserializer.configure(configs, isKey);
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<Object> serializer() {
        return this.serializer;
    }

    @Override
    public Deserializer<Object> deserializer() {
        return this.deserializer;
    }
}
