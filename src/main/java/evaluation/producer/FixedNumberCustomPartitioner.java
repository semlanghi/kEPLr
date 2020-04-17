package evaluation.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * Partitioner used to define the partitioning adapted to the asymmetric
 * configuration of the number of partitions and number of keys.
 */
public class FixedNumberCustomPartitioner implements Partitioner {


    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer tk = Integer.parseInt(((String) key).split("-")[1]);
        Integer np = tk % WProducerBase.PARTITIONS;
        return cluster.partitionsForTopic(topic).get(np).partition();
    }
}
