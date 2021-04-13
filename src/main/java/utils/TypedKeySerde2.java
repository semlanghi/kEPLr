package utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroModule;
import lombok.extern.log4j.Log4j;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.keplr.etype.TypedKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.stream.Stream;

/**
 * A {@link Serde} for the {@link TypedKey} representation. It uses an {@link ObjectMapper} to
 * first convert the value as a string. And then create a bytes representation based on that.
 *
 * @see ObjectMapper
 * @param <K>
 */

@Log4j
public class TypedKeySerde2<K> implements Serde<TypedKey<K>> {


    private final Serde<K> originalKeySerde;
    private final Serde<String> stringSerde = Serdes.String();

    public TypedKeySerde2(Serde<K> originalKeySerde) {
        this.originalKeySerde = originalKeySerde;
    }

    public Serde<K> getOriginalKeySerde() {
        return originalKeySerde;
    }

    @Override
    public Serializer<TypedKey<K>> serializer() {
        return (topic, data) -> {

            byte[] serializedData = originalKeySerde.serializer().serialize(topic,data.getKey());
            byte[] serializedTypeName = stringSerde.serializer().serialize(topic, data.getType());
            byte[] interleaving = new byte[]{0};

            byte[] interleavedTypeName = ArrayUtils.addAll(interleaving,serializedTypeName);
            return ArrayUtils.addAll(serializedData, interleavedTypeName);
        };
    }

    @Override
    public Deserializer<TypedKey<K>> deserializer() {
        return (topic, data) -> {

            int index = 0;
            for (int i=0; i<data.length;i++
                 ) {
                byte t = 0;
                if(data[i] == t){
                    index = i;
                }
            }

            byte[] serializedOriginalKey = Arrays.copyOfRange(data,0,index);
            byte[] serializedTypeName = Arrays.copyOfRange(data,index+1,data.length);

            return new TypedKey<>(originalKeySerde.deserializer().deserialize(topic,serializedOriginalKey),
                    stringSerde.deserializer().deserialize(topic, serializedTypeName));
        };
    }
}
