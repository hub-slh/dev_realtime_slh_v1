package com.slh.Type;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.function.ProcessJoinBase2And4BaseFunc;
import com.slh.function.ProcessLabelFunc;
import com.slh.utils.EnvironmentSettingUtils;
import com.slh.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;
import java.util.Date;


/**
 * @Package com.slh.Type.Tag_realtime1
 * @Author song.lihao
 * @Date 2025/5/15 20:39
 * @description:
 */
public class Tag_realtime1 {
    // Kafka地址
    private static final String kafka_botstrap_servers = "cdh01:9092";
    // kafka 中的主题 kafka_result_label_base6
    private static final String kafka_label_base6_topic = "kafka_result_label_base6";
    // kafka 中的主题 kafka_result_label_base4
    private static final String kafka_label_base4_topic = "kafka_result_label_base4";
    // kafka 中的主题 kafka_result_label_base2
    private static final String kafka_label_base2_topic = "kafka_result_label_base2";

    @SneakyThrows
    public static void main(String[] args) {
        // 设置 Hadoop 用户名 root
        System.setProperty("HADOOP_USER_NAME", "root");
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度
        env.setParallelism(2);
        // 从 Kafka 的 kafka_result_label_base6 主题读取数据
        SingleOutputStreamOperator<String> kafkaBase6Source = env.fromSource(
                // 构建 Kafka 安全数据源
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base6_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()// 从最早的偏移量开始消费
                        ),
                        // 构建水印策略
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            // 将事件解析为 JSON 对象
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            // 检查事件是否不为空且包含 ts_ms 字段
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    // 从 JSON 对象获取 ts_ms 字段的值作为时间戳
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    // 异常信息打印
                                                    e.printStackTrace();
                                                    // 打印解析失败的事件信息
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base6_topic_source"// 数据源的名称
                ).uid("kafka_base6_source")// 设置数据源的唯一ID
                .name("kafka_base6_source");// 设置数据源的名称
        // 从 Kafka 的 kafka_result_label_base4 读取数据
        SingleOutputStreamOperator<String> kafkaBase4Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base4_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base4_topic_source"
                ).uid("kafka_base4_source")
                .name("kafka_base4_source");
        // 从 Kafka 的 kafka_result_label_base2 读取数据
        SingleOutputStreamOperator<String> kafkaBase2Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base2_topic,
                                new Date().toString(),
                                OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((event, timestamp) -> {
                                            JSONObject jsonObject = JSONObject.parseObject(event);
                                            if (event != null && jsonObject.containsKey("ts_ms")){
                                                try {
                                                    return JSONObject.parseObject(event).getLong("ts_ms");
                                                }catch (Exception e){
                                                    e.printStackTrace();
                                                    System.err.println("Failed to parse event as JSON or get ts_ms: " + event);
                                                    return 0L;
                                                }
                                            }
                                            return 0L;
                                        }
                                ),
                        "kafka_label_base2_topic_source"
                ).uid("kafka_base2_source")
                .name("kafka_base2_source");
        // 将从 Kafka 读取的字符串数据转换为 JSON 对象
        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = kafkaBase6Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = kafkaBase4Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = kafkaBase2Source.map(JSON::parseObject);
        // 将 base2 和 base4 的数据按照 uid 进行关联，时间范围为前后 24 小时
        SingleOutputStreamOperator<JSONObject> join2_4Ds = mapBase2LabelDs.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase4LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessJoinBase2And4BaseFunc());
        // 为关联后的数据设置水印
        SingleOutputStreamOperator<JSONObject> waterJoin2_4 = join2_4Ds.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<JSONObject>) (jsonObject, l) -> jsonObject.getLongValue("ts_ms")));
        // 将关联后的数据和 base6 的数据按照 uid 进行关联，时间范围为前后 24 小时
        SingleOutputStreamOperator<JSONObject> userLabelProcessDs = waterJoin2_4.keyBy(o -> o.getString("uid"))
                .intervalJoin(mapBase6LabelDs.keyBy(o -> o.getString("uid")))
                .between(Time.hours(-24), Time.hours(24))
                .process(new ProcessLabelFunc());


        userLabelProcessDs.print();


        env.execute();
    }
}
