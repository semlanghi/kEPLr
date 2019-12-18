package examples.typedstream;

import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.ValueJoiner;

public interface KTStream<K,V,T> extends KStream<K,V>{



    KTStream<K,V,T> match(KStream<K,V> stream, EType<V,T> type);

    <VO, VR> KStream<K, VR> followedBy(final KTStream<K,VO,T> otherStream,
                                 final ValueJoiner<? super V, ? super VO, ? extends VR> joiner,
                                 final JoinWindows windows);





}
