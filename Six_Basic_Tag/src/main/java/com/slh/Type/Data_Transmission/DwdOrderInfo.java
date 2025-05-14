package com.slh.Type.Data_Transmission;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.utils.FlinkSourceUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Package com.lzy.app.dwd.DwdOrderInfo
 * @Author zheyuan.liu
 * @Date 2025/5/14 19:21
 * @description:
 */

public class DwdOrderInfo {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);

        KafkaSource<String> kafkaSourceBd = FlinkSourceUtil.getKafkaSource("topic_db_v1", "dwd_app");

        DataStreamSource<String> kafka_source = env.fromSource(kafkaSourceBd, WatermarkStrategy.noWatermarks(), "Kafka Source");

        SingleOutputStreamOperator<JSONObject> streamOperator = kafka_source.map(JSON::parseObject).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts_ms");
                    }
                }));

        SingleOutputStreamOperator<JSONObject> orderDs = streamOperator.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"));

        SingleOutputStreamOperator<JSONObject> detailDs = streamOperator.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"));

        SingleOutputStreamOperator<JSONObject> operator = orderDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject rebuce = new JSONObject();
                JSONObject after = value.getJSONObject("after");
                rebuce.put("order_id", after.getString("id"));
                rebuce.put("user_id", after.getString("user_id"));
                rebuce.put("total_amount", after.getString("total_amount"));
                rebuce.put("create_time", after.getString("create_time"));
                out.collect(rebuce);
            }
        });

        SingleOutputStreamOperator<JSONObject> detail = detailDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject rebuce = new JSONObject();
                JSONObject after = value.getJSONObject("after");
                rebuce.put("order_id", after.getString("order_id"));
                rebuce.put("sku_id", after.getString("sku_id"));
                rebuce.put("sku_name", after.getString("sku_name"));
                out.collect(rebuce);
            }
        });

        KeyedStream<JSONObject, String> idBy = operator.keyBy(data -> data.getString("id"));

        KeyedStream<JSONObject, String> oidBy = detail.keyBy(data -> data.getString("order_id"));

        idBy.intervalJoin(oidBy).between(Time.minutes(-5) , Time.minutes(5)).process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject left, JSONObject right, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject merged = new JSONObject();
                // 添加用户基本信息（来自 UserInfoDS）
                merged.put("id", left.getString("id"));
                merged.put("user_id", left.getString("user_id"));
                merged.put("total_amount", left.getString("total_amount"));
                merged.put("create_time", left.getString("create_time"));

                // 添加用户补充信息（来自 UserInfoSupDS）
                merged.put("order_id", right.getString("order_id"));
                merged.put("sku_id", right.getString("sku_id"));
                merged.put("sku_name", right.getString("sku_name"));

                out.collect(merged);
            }
        });

        operator.print();

        env.execute("DwdOrderInfo");
    }
}
