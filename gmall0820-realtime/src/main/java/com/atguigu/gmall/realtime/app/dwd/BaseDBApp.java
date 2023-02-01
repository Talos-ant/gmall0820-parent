package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * TODO
 *
 * @Description
 * @Author talos
 * @Date 2023/2/1 3:46 PM
 **/
public class BaseDBApp {
    public static void main(String[] args) throws Exception {
        // todo 1.准备环境
        // 1.1 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.2 设置并行度,一般与kafka分区数一致
        env.setParallelism(4);
        // 1.3 为保证精准一次消费,设置checkpoint,并设置超时时间,以及状态后端
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:9820/gmall/checkpoint/baseDBApp"));

        // todo 2.从kafka ods层读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";
        // 2.1 通过工具类获取kafka消费者
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonStrDS = env.addSource(kafkaSource);

        // todo 3.转化格式
        SingleOutputStreamOperator<JSONObject> jsonObjDS = jsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr));

        // todo 4.对数据进行ETL,如果table 为空 或者data为空,或者长度小于3,过滤
        SingleOutputStreamOperator<JSONObject> filteredDS = jsonObjDS.filter(
                jsonObj -> {
                    boolean flag = jsonObj.getString("table") != null
                            && jsonObj.getJSONObject("data") != null
                            && jsonObj.getString("data").length() >= 3;
                    return flag;
                }
        );

        filteredDS.print(">>>>>>>>>>>>>");

        env.execute();

    }
}
