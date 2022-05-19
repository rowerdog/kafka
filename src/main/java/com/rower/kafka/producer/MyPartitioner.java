package com.rower.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * @ClassName: MyPartitioner
 * @Description
 * @Author gengmb on 2022/5/19 16:46
 * @Version: 1.0
 */
public class MyPartitioner implements Partitioner {
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {

        String msgValue = value.toString();
        int partition = 0;
        if (msgValue.contains("dollar")) {
            partition = 0;
        } else {
            partition = 1;
        }
        return partition;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
