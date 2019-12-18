package examples.typedstream;

import org.apache.kafka.streams.kstream.KStream;

public abstract class EType<E,T> {

    T type;

    public abstract boolean check(E stream);

}
