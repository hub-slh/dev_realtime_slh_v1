package com.slh.Type.Data_Transmission;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.time.Duration;
import java.util.*;

/**
 * @Package com.slh.Type.Data_Transmission.DwdLogApp
 * @Author song.lihao
 * @Date 2025/5/14 21:03
 * @description:
 */
public class DwdLogApp {
    public static void main(String[] args) throws Exception{
        // 初始化 Flink 流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 从 Kafka 获取日志消费
        KafkaSource<String> kafkaSourceLog = FlinkSourceUtil.getKafkaSource("topic_log", "dwd_app");
        DataStreamSource<String> kafka_source_log = env.fromSource(kafkaSourceLog, WatermarkStrategy.noWatermarks(), "kafka Source");
        // 将 Json 自读穿转换为 JSONObject 并分配时间戳
        SingleOutputStreamOperator<JSONObject> streamOperatorlog = kafka_source_log.map(JSON::parseObject)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");// 使用日志中的 ts 字段作为事件时间戳
                            }
                        }));
        // 数据清洗和转换：提取设备信息和搜索词
        SingleOutputStreamOperator<JSONObject> logDeviceInfoDs = streamOperatorlog.map(new MapFunction<JSONObject, JSONObject>() {
            @Override
            public JSONObject map(JSONObject jsonObject) {
                JSONObject result = new JSONObject();
                if (jsonObject.containsKey("common")) {
                    JSONObject common = jsonObject.getJSONObject("common");
                    // 提取用户 ID
                    result.put("uid", common.getString("uid") != null ? common.getString("uid") : "-1");
                    result.put("ts", jsonObject.getLongValue("ts"));
                    // 提取设备信息，去除不需要的字段
                    JSONObject deviceInfo = new JSONObject();
                    common.remove("sid");
                    common.remove("mid");
                    common.remove("is_new");
                    deviceInfo.putAll(common);
                    result.put("deviceInfo", deviceInfo);
                    // 如果是关键词搜索页面，提取搜索词
                    if (jsonObject.containsKey("page") && !jsonObject.getJSONObject("page").isEmpty()) {
                        JSONObject pageInfo = jsonObject.getJSONObject("page");
                        if (pageInfo.containsKey("item_type") && pageInfo.getString("item_type").equals("keyword")) {
                            String item = pageInfo.getString("item");
                            result.put("search_item", item);
                        }
                    }
                }
                // 处理操作系统字段
                JSONObject deviceInfo = result.getJSONObject("deviceInfo");
                String os = deviceInfo.getString("os").split(" ")[0];
                deviceInfo.put("os", os);

                return result;
            }
        });
        // 过滤 UID 为空的数据
        SingleOutputStreamOperator<JSONObject> filterNotNullUidLogPageMsg = logDeviceInfoDs.filter(data -> !data.getString("uid").isEmpty());
        // 按照用户 ID 进行分组
        KeyedStream<JSONObject, String> keyedStreamLogPageMsg = filterNotNullUidLogPageMsg.keyBy(data -> data.getString("uid"));
        // 数据去重处理：使用状态存储已处理的数据，避免重复处理数据
        SingleOutputStreamOperator<JSONObject> processStagePageLogDs = keyedStreamLogPageMsg.process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

            private Logger LOG = LoggerFactory.getLogger(String.class);
            private ValueState<HashSet<String>> processedDataState;// 状态存储已处理的数据

            @Override
            public void open(Configuration parameters) {
                // 初始化状态描述符
                ValueStateDescriptor<HashSet<String>> descriptor = new ValueStateDescriptor<>(
                        "processedDataState",
                        TypeInformation.of(new org.apache.flink.api.common.typeinfo.TypeHint<HashSet<String>>() {})
                );
                processedDataState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public void processElement(JSONObject value, Context ctx, Collector<JSONObject> out) throws Exception {
                HashSet<String> processedData = processedDataState.value();
                if (processedData == null) {
                    processedData = new HashSet<>();
                }

                String dataStr = value.toJSONString();
                LOG.info("Processing data: {}", dataStr);
                if (!processedData.contains(dataStr)) {
                    LOG.info("Adding new data to set: {}", dataStr);
                    processedData.add(dataStr);
                    processedDataState.update(processedData);
                    out.collect(value);// 只输出未处理过数据
                } else {
                    LOG.info("Duplicate data found: {}", dataStr);
                }
            }
        });
//        logDeviceInfoDs.print();
        // 用户行为指标统计： 计算pv收集设备信息和搜索词
        SingleOutputStreamOperator<JSONObject> win2MinutesPageLogsDs = processStagePageLogDs.keyBy(value -> value.getString("uid"))
                .process(new KeyedProcessFunction<String, JSONObject, JSONObject>() {

                    ValueState<Long> pvState;// PV计数器状态
                    MapState<String, Set<String>> uvState;// 设备信息和搜索词集合状态

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        // 初始化 PV 状态
                        pvState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("pv", Long.class));

                        // 初始化字段集合状态（使用TypeHint保留泛型信息）
                        MapStateDescriptor<String, Set<String>> fieldsDescriptor =
                                new MapStateDescriptor<>("fields-state", Types.STRING, TypeInformation.of(new TypeHint<Set<String>>() {}));

                        uvState = getRuntimeContext().getMapState(fieldsDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject value, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        // 更新PV
                        Long pv = pvState.value() == null ? 1L : pvState.value() + 1;
                        pvState.update(pv);

                        // 提取设备信息和搜索词
                        JSONObject deviceInfo = value.getJSONObject("deviceInfo");
                        String os = deviceInfo.getString("os");
                        String ch = deviceInfo.getString("ch");
                        String md = deviceInfo.getString("md");
                        String ba = deviceInfo.getString("ba");
                        String searchItem = value.containsKey("search_item") ? value.getString("search_item") : null;

                        // 更新字段集合
                        updateField("os", os);
                        updateField("ch", ch);
                        updateField("md", md);
                        updateField("ba", ba);
                        if (searchItem != null) {
                            updateField("search_item", searchItem);
                        }

                        // 构建输出JSON
                        JSONObject output = new JSONObject();
                        output.put("uid", value.getString("uid"));
                        output.put("pv", pv);
                        output.put("os", String.join(",", getField("os")));
                        output.put("ch", String.join(",", getField("ch")));
                        output.put("md", String.join(",", getField("md")));
                        output.put("ba", String.join(",", getField("ba")));
                        output.put("search_item", String.join(",", getField("search_item")));

                        out.collect(output);
                    }

                    // 辅助方法：更新字段集合
                    private void updateField(String field, String value) throws Exception {
                        Set<String> set = uvState.get(field) == null ? new HashSet<>() : uvState.get(field);
                        set.add(value);
                        uvState.put(field, set);
                    }

                    // 辅助方法：获取字段集合
                    private Set<String> getField(String field) throws Exception {
                        return uvState.get(field) == null ? Collections.emptySet() : uvState.get(field);
                    }
                });
        // 每两分钟窗口内取最新结果
        SingleOutputStreamOperator<JSONObject> reduce = win2MinutesPageLogsDs.keyBy(data -> data.getString("uid"))
                .window(TumblingProcessingTimeWindows.of(Time.minutes(2)))
                .reduce((value1, value2) -> value2);
        // 将结果转换为字符串并打印
        SingleOutputStreamOperator<String> operator = reduce.map(data -> data.toString());

        operator.print();

//        operator.sinkTo(FlinkSinkUtil.getKafkaSink("minutes_page_Log"));

        env.execute("DwdLogApp");
    }
}
