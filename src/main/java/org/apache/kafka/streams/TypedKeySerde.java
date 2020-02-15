package org.apache.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroModule;
import jdk.internal.org.objectweb.asm.TypeReference;
import org.apache.kafka.common.serialization.*;

import java.io.IOException;
import java.io.Serializable;

public class TypedKeySerde<K> implements Serde<TypedKey<K>> {

    private ObjectMapper mapper = new ObjectMapper();
    private Class<K> contentClass;

    public TypedKeySerde(Class<K> kClass) {
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

        return new Deserializer<TypedKey<K>>() {
            @Override
            public TypedKey<K> deserialize(String topic, byte[] data) {
                try {
                    return mapper.readValue(data,  mapper.getTypeFactory().constructParametricType(TypedKey.class, contentClass));
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        };
    }
}
