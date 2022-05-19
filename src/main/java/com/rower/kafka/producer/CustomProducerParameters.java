package com.rower.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 *
 *
 * 生产者提高吞吐量
 *
 * - batch.size：批次大小，默认16k
 *
 * - linger.ms：等待时间，修改为5-100ms
 * - compression.type：压缩snappy
 * - RecordAccumulator：缓冲区大小，修改为64m
 *
 * @ClassName: CustomProducerParameters
 * @Description
 * @Author gengmb on 2022/5/19 17:11
 * @Version: 1.0
 */
public class CustomProducerParameters {

    public static void main(String[] args) {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"bigdata9:9092,bigdata10:9092,bigdata11:9092,bigdata12:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //batch.size 默认是16k
        properties.put(ProducerConfig.BATCH_SIZE_CONFIG,16384);

        //linger.ms 默认是0秒，建议修改为5-100ms
        properties.put(ProducerConfig.LINGER_MS_CONFIG,1);

        //recordAccumulator
        //发送缓冲区大小，默认是32 M
        properties.put(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        properties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        KafkaProducer producer = new KafkaProducer<String,String>(properties);


        //同步发送只需要后面加一个get()
        //这批数据发送完之后发送下一批
        for(int i = 0 ; i < 5 ; i++){
            producer.send(new ProducerRecord("first", "hello" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if(null == exception){
                        System.out.printf("主题" + metadata.topic() + "分区" + metadata.partition());
                    }
                }
            });
        }

        producer.close();
    }
}
