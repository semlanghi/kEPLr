//import com.sun.deploy.util.ArrayUtil;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.keplr.etype.TypedKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import utils.TypedKeySerde2;

import java.math.BigInteger;

import static junit.framework.Assert.assertEquals;

public class TestTypedKeyIntegerSerde {

    private Serde<TypedKey<Integer>> serde;

    @BeforeEach
    public void setUp(){
        this.serde = new TypedKeySerde2<>(Serdes.Integer());
    }

    @Test
    public void testSerde(){
        Integer originalKey = 5;
        String topic = "topic";
        TypedKey<Integer> typedKey = new TypedKey<>(originalKey, "TypeA");

        byte[] serializedKey = serde.serializer().serialize(topic, typedKey);

        TypedKey<Integer> otherTypedKey = serde.deserializer().deserialize(topic,serializedKey);

        assertEquals(typedKey,otherTypedKey);
    }

    @Test
    public void testDeser(){
        int originalKey = 5;
        String type = "TypeA";
        String topic = "topic";

        byte[] interleavedType = ArrayUtils.addAll(new byte[]{0},type.getBytes());
        byte[] serializedKey = ArrayUtils.addAll(new byte[]{0, 0, 0, 5},interleavedType);

        TypedKey<Integer> deserializedKey = serde.deserializer().deserialize(topic, serializedKey);

        TypedKey<Integer> typedKey = new TypedKey<>(originalKey, "TypeA");
        assertEquals(typedKey, deserializedKey);

        byte[] reselializedKey = serde.serializer().serialize(topic, deserializedKey);
        assert(Bytes.BYTES_LEXICO_COMPARATOR.compare(reselializedKey, serializedKey)==0);

    }
}
