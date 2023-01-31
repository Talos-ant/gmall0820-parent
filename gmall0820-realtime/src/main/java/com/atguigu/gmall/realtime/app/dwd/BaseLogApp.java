package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.common.internals.Topic;

/**
 * TODO
 * 准备用户行为日志
 * @Description
 * @Author talos
 * @Date 2023/1/31 11:12 AM
 **/
public class BaseLogApp {
    public static void main(String[] args) throws Exception {
        // todo 1.准备环境
        // 1.1创建Flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2设置并行度
        env.setParallelism(4);

        // todo 2.从kafka中读取数据
        // 2.1 调用kafka工具类,获取FlinkKafkaConsumer
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        // todo 3.对读取到的数据进行转换 string->json
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.map(
                new MapFunction<String, JSONObject>() {
                    @Override
                    public JSONObject map(String value) throws Exception {
                        JSONObject jsonObject = JSON.parseObject(value);

                        return jsonObject;
                    }
                }
        );

        jsonObjectDS.print();


        env.execute("dwd_base_log job");


    }
}
