package com.rower.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @ClassName: CustomProducer
 * @Description
 * @Author gengmb on 2022/5/19 9:45
 * @Version: 1.0
 */
public class CustomProducer {

    public static void main(String[] args) {

        Properties properties = new Properties();

        //建议使用注解的方式，防止写错，写多个主机防止单个主机出现故障
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"bigdata9:9092,bigdata10:9092,bigdata11:9092,bigdata12:9092");

        //key value序列化使用的是kafka自己提供的序列化
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer producer = new KafkaProducer<String,String>(properties);

        for(int i = 0 ; i < 5 ; i++){
            producer.send(new ProducerRecord("kafka_springboot",i+""));
        }

        producer.close();
    }
}
