package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroModule;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.keplr.etype.TypedKey;

import java.io.IOException;

/**
 * A {@link Serde} for the {@link TypedKey} representation. It uses an {@link ObjectMapper} to
 * first convert the value as a string. And then create a bytes representation based on that.
 *
 * @see ObjectMapper
 * @param <K>
 */
public class JacksonTypedKeySerde<K> implements Serde<TypedKey<K>> {

    private ObjectMapper mapper = new ObjectMapper();
    private Class<K> contentClass;

    public JacksonTypedKeySerde(Class<K> kClass) {
        this.contentClass = kClass;
        mapper.enableDefaultTyping();
        mapper.registerModule(new AvroModule());
    }


    @Override
    public Serializer<TypedKey<K>> serializer() {
        return (topic, data) -> {
            try {
                return mapper.writeValueAsString(data).getBytes();

            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
            return null;
        };
    }

    @Override
    public Deserializer<TypedKey<K>> deserializer() {

        return (topic, data) -> {
            try {
                return mapper.readValue(data,  mapper.getTypeFactory().constructParametricType(TypedKey.class, contentClass));
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        };
    }
}
