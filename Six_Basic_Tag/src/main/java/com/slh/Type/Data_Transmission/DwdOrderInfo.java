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
 * @Package com.slh.Type.Data_Transmission
 * @Author song.lihao
 * @Date 2025/5/14 19:21
 * @description:
 */

public class DwdOrderInfo {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置
        env.setParallelism(1);
        // 创建 Kafka 数据源 消费 topic_db_v1 主题 消费者组为 dwd_app
        KafkaSource<String> kafkaSourceBd = FlinkSourceUtil.getKafkaSource("topic_db_v1", "dwd_app");
        // 从 Kafka 创建数据流
        DataStreamSource<String> kafka_source = env.fromSource(kafkaSourceBd, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 数据流预处理
        SingleOutputStreamOperator<JSONObject> streamOperator = kafka_source.map(JSON::parseObject).assignTimestampsAndWatermarks(WatermarkStrategy
                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        // 使用 JSON 中的 ts_ms 字段作为事件时间戳
                        return element.getLong("ts_ms");
                    }
                }));
        // 过滤出订单表数据
        SingleOutputStreamOperator<JSONObject> orderDs = streamOperator.filter(data -> data.getJSONObject("source").getString("table").equals("order_info"));
        // 过滤出订单明细表数据
        SingleOutputStreamOperator<JSONObject> detailDs = streamOperator.filter(data -> data.getJSONObject("source").getString("table").equals("order_detail"));
        // 订单数据字段提取处理
        SingleOutputStreamOperator<JSONObject> operator = orderDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject rebuce = new JSONObject();
                JSONObject after = value.getJSONObject("after");  // 获取变更后的数据
                // 提取关键字段
                rebuce.put("order_id", after.getString("id"));  // 订单ID
                rebuce.put("user_id", after.getString("user_id"));  // 用户ID
                rebuce.put("total_amount", after.getString("total_amount"));  // 订单金额
                rebuce.put("create_time", after.getString("create_time"));  // 创建时间
                out.collect(rebuce);
            }
        });
//      ------ ++++++ 获取订单金额，用户ID，订单ID ++++++ ------
//        operator.print();
        // 订单明细数据字段提取处理
        SingleOutputStreamOperator<JSONObject> detail = detailDs.process(new ProcessFunction<JSONObject, JSONObject>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                JSONObject rebuce = new JSONObject();
                JSONObject after = value.getJSONObject("after"); // 获取变更后的数据
                // 提取关键字段
                rebuce.put("order_id", after.getString("order_id"));  // 关联订单ID
                rebuce.put("sku_id", after.getString("sku_id"));  // 商品ID
                rebuce.put("sku_name", after.getString("sku_name"));  // 商品名称
                out.collect(rebuce);
            }
        });
//      ------ ++++++ 获取关联订单ID，商品ID，商品名称 ++++++ ------
//        detail.print();
        // 按键分区（订单主表按order_id）
        KeyedStream<JSONObject, String> idBy = operator.keyBy(data -> data.getString("order_id"));
        // 按键分区（订单明细表按order_id）
        KeyedStream<JSONObject, String> oidBy = detail.keyBy(data -> data.getString("order_id"));
        // 双流区间连接（5分钟时间窗口）
        SingleOutputStreamOperator<JSONObject> process = idBy.intervalJoin(oidBy)
                .between(Time.minutes(-5), Time.minutes(5))  // 允许前后5分钟的关联
                .process(new ProcessJoinFunction<JSONObject, JSONObject, JSONObject>() {
                    @Override
                    public void processElement(JSONObject left, JSONObject right, Context ctx, Collector<JSONObject> out) {
                        JSONObject merged = new JSONObject();
                        // 合并订单主表字段
                        merged.put("order_id", left.getString("order_id"));
                        merged.put("user_id", left.getString("user_id"));
                        merged.put("total_amount", left.getString("total_amount"));
                        merged.put("create_time", left.getString("create_time"));
                        // 合并订单明细字段
                        merged.put("sku_id", right.getString("sku_id"));
                        merged.put("sku_name", right.getString("sku_name"));
                        out.collect(merged);  // 输出合并结果
                    }
                });
        process.print();
        // 启动作业
        env.execute("DwdOrderInfo");
    }
}
