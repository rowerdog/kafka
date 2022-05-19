package com.rower.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


/**
 *  kafka异步发送，带回调函数的异步发送
 *  发送失败kafka会自动重试，不需要我们手动重试
 *
 *
 *
 *
 * @ClassName: CustomProducerCallback
 * @Description
 * @Author gengmb on 2022/5/19 10:15
 * @Version: 1.0
 */
public class CustomProducerCallbackPartitions {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties properties = new Properties();

        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"bigdata9:9092,bigdata10:9092,bigdata11:9092,bigdata12:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.rower.kafka.producer.MyPartitioner");

        KafkaProducer producer = new KafkaProducer<String,String>(properties);



        //同步发送只需要后面加一个get()
        //这批数据发送完之后发送下一批
        for(int i = 0 ; i < 5 ; i++){
            producer.send(new ProducerRecord("fisrt",  "dollar" + i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("主题" + metadata.topic() + "分区" + metadata.partition());
                }
            });

        }

        producer.close();
    }
}
