package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.common.internals.Topic;

import java.text.SimpleDateFormat;
import java.util.Date;

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
        // 1.3设置checkpoint ,默认exactly_once
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop202:9820/gmall/checkpoint/baselogApp"));


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

        // jsonObjectDS.print();
        // todo 4.识别新老访客
        // 将首次访问日期作为状态保存起来,和后面日志过来的日期进行对比

        // 4.1 根据mid对日志进行分组
        KeyedStream<JSONObject, String> midKeyedDS = jsonObjectDS.keyBy(
                data -> data.getJSONObject("common").getString("mid")
        );
        // 4.2 新老用户状态修复,防止出现假新用户
        // 这里需要用到状态后端来保存用户第一次的访问时间,状态分为算子状态(每个算子的子任务一个状态)和键控状态(每个元素一个状态),
        // 我们这里使用键控状态合适,需要对keyedBy之后的流操作,细分为ValueState,ListState
        SingleOutputStreamOperator<JSONObject> jsonDSWithFlag = midKeyedDS.map(
                // 需要用到生命周期函数,所以需要使用rich函数
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 定义该mid访问状态
                    private ValueState<String> firstVisitDateState;
                    // 定义日期格式化对象
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 对状态以及日期格式进行初始化
                        firstVisitDateState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("newMidDateState", String.class)
                        );
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        // 获取当前日志标记状态
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");

                        // 获取当前日志访问时间戳
                        Long ts = jsonObject.getLong("ts");

                        if ("1".equals(isNew)) {
                            // 获取当前mid对象的状态
                            String stateDate = firstVisitDateState.value();
                            // 对当前的日期进行转化
                            String curDate = sdf.format(new Date(ts));
                            // 如果状态不为空,并且状态日期和当前日期不相等,说明是老访客
                            if (StringUtils.isNotBlank(stateDate)) {
                                // 不为空,再比较时间是否一致
                                if (!stateDate.equals(curDate)) {
                                    // 重新赋值
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                // 如果为空,保存状态
                                firstVisitDateState.update(curDate);
                            }

                        }

                        return jsonObject;
                    }
                }
        );

        // jsonDSWithFlag.print(">>>>>>>>>>>>>>>>");
        // todo 根据日志数据内容,将日志数据分为3类, 页面日志、启动日志和曝光日志。页面日志输出到主流,
        //  启动日志输出到启动侧输出流,曝光日志输出到曝光日志侧输出流
        //  侧输出流:1)接受迟到数据 2)分流 需要使用process 底层函数,利用其中的上下文来获取侧输出流
        // todo context.output() 用来输出侧输出流,collector.collect 用来收集主流
        // 定义启动侧输出流标签 有坑,需要注意 需要加{},前面指定泛型
        OutputTag<String> startTag = new OutputTag<String>("start"){};
        // 定义曝光侧输出流标签
        OutputTag<String> displayTag = new OutputTag<String>("display"){};

        SingleOutputStreamOperator<String> pageDS = jsonDSWithFlag.process(
                // 改为string 重新写入kafka
                new ProcessFunction<JSONObject, String>() {

                    @Override
                    public void processElement(JSONObject jsonObject,
                                               ProcessFunction<JSONObject, String>.Context context,
                                               Collector<String> collector) throws Exception {
                        // 获取启动日志标记
                        JSONObject startJsonObj = jsonObject.getJSONObject("start");
                        // 判断是否为启动日志
                        if (startJsonObj != null && startJsonObj.size() > 0) {
                            // 如果是启动日志,输出到启动侧输出流
                            context.output(startTag, jsonObject.toString());

                        } else {
                            // 如果不是启动日志,获取曝光日志
                            JSONArray displays = jsonObject.getJSONArray("displays");
                            if (displays != null && displays.size() > 0) {
                                // 如果是曝光日志,遍历输出到侧输出流
                                for (int i = 0; i < displays.size(); i++) {
                                    JSONObject displaysObj = displays.getJSONObject(i);
                                    String pageId = jsonObject.getJSONObject("page").getString("page_id");
                                    displaysObj.put("page_id", pageId);
                                    context.output(displayTag, displaysObj.toString());
                                }
                            } else {
                                // 如果不是曝光日志,说明是页面日志,输出到主流
                                collector.collect(jsonObject.toString());
                            }
                        }


                    }
                }
        );

        // 获取侧输出流
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);

        // 打印输出
        pageDS.print("page>>>>>>>>>>>>");
        startDS.print("start>>>>>>>>>>>>");
        displayDS.print("display>>>>>>>>>>>>");


        env.execute("dwd_base_log job");


    }
}
