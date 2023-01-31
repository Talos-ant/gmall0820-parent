package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * TODO
 *
 * @Description 操作kafka的工具类
 * @Author talos
 * @Date 2023/1/31 11:46 AM
 **/
public class MyKafkaUtil {
    private static String kafkaServer = "hadoop202:9092,hadoop203:9092,hadoop204:9092";
    // 获取FlinkKafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaSource(String topic,String groupId){
        // kafka 连接的属性配置
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,topic);
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);
        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),props);
    }

    public static FlinkKafkaProducer<String> getKafkaSink(String topic){
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaServer);

        return new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),props);
    }
}
