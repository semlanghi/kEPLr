package evaluation.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {

    @Override
    public void configure(Map<String, ?> configs) {
    }

    @Override
    public void close() {
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        Integer tk = Integer.parseInt(((String) key).split("-")[1]);
        return cluster.partitionsForTopic(topic).get(tk).partition();
    }

}