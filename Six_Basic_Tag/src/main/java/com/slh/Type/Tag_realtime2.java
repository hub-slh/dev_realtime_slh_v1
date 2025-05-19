package com.slh.Type;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.function.ProcessJoinBase2AndBase4Func;
import com.slh.function.ProcessJoinBase6LabelFunc;
import com.slh.utils.ConfigUtils;
import com.slh.utils.EnvironmentSettingUtils;
import com.slh.utils.KafkaUtils;
import com.slh.utils.WaterMarkUtils;
import lombok.SneakyThrows;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Date;

/**
 * @Package com.slh.Type.Tag_realtime2
 * @Author song.lihao
 * @Date 2025/5/18 19:13
 * @description: 该类主要实现了从 Kafka 主题中读取数据，进行数据处理和关联操作，
 * 最后将处理结果写回到 Kafka 主题的实时流处理任务。
 */
public class Tag_realtime2 {
    // 从配置文件中获取 Kafka 服务器地址
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    // 从配置文件中获取 Kafka 主题名称，用于存储标签基础 6 数据
    private static final String kafka_label_base6_topic = ConfigUtils.getString("kafka.result.label.base6.topic");
    // 从配置文件中获取 Kafka 主题名称，用于存储标签基础 4 数据
    private static final String kafka_label_base4_topic = ConfigUtils.getString("kafka.result.label.base4.topic");
    // 从配置文件中获取 Kafka 主题名称，用于存储标签基础 2 数据
    private static final String kafka_label_base2_topic = ConfigUtils.getString("kafka.result.label.base2.topic");
    // 从配置文件中获取 Kafka 主题名称，用于存储用户标签基线数据
    private static final String kafka_label_user_baseline_topic = ConfigUtils.getString("kafka.result.label.baseline.topic");

    @SneakyThrows
    public static void main(String[] args) {
        // 设置 Hadoop 用户名为 root
        System.setProperty("HADOOP_USER_NAME", "root");

        // 获取 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置环境的默认参数
        EnvironmentSettingUtils.defaultParameter(env);

        // 从 Kafka 主题 kafka_label_base6_topic 中读取数据
        SingleOutputStreamOperator<String> kafkaBase6Source = env.fromSource(
                        // 构建 Kafka 安全数据源
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base6_topic,
                                new Date() + "_kafkaBase6Source",
                                OffsetsInitializer.earliest() // 从最早的偏移量开始消费
                        ),
                        // 分配水印策略，使用 "ts_ms" 字段作为时间戳，允许 3 秒的乱序
                        WaterMarkUtils.publicAssignWatermarkStrategy("ts_ms", 3),
                        "kafka_label_base6_topic_source"
                ).uid("kafka_base6_source")
                .name("kafka_base6_source");

        // 从 Kafka 主题 kafka_label_base4_topic 中读取数据
        SingleOutputStreamOperator<String> kafkaBase4Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base4_topic,
                                new Date() + "_kafkaBase4Source",
                                OffsetsInitializer.earliest()
                        ),
                        WaterMarkUtils.publicAssignWatermarkStrategy("ts_ms", 3),
                        "kafka_label_base4_topic_source"
                ).uid("kafka_base4_source")
                .name("kafka_base4_source");

        // 从 Kafka 主题 kafka_label_base2_topic 中读取数据
        SingleOutputStreamOperator<String> kafkaBase2Source = env.fromSource(
                        KafkaUtils.buildKafkaSecureSource(
                                kafka_botstrap_servers,
                                kafka_label_base2_topic,
                                new Date() + "_kafkaBase2Source",
                                OffsetsInitializer.earliest()
                        ),
                        WaterMarkUtils.publicAssignWatermarkStrategy("ts_ms", 3),
                        "kafka_label_base2_topic_source"
                ).uid("kafka_base2_source")
                .name("kafka_base2_source");

        // 将从 Kafka 读取的字符串数据解析为 JSONObject
        SingleOutputStreamOperator<JSONObject> mapBase6LabelDs = kafkaBase6Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase4LabelDs = kafkaBase4Source.map(JSON::parseObject);
        SingleOutputStreamOperator<JSONObject> mapBase2LabelDs = kafkaBase2Source.map(JSON::parseObject);

        // 按照 "uid" 字段对数据进行分组
        KeyedStream<JSONObject, String> keyedStreamBase2LabelDs = mapBase2LabelDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamBase4LabelDs = mapBase4LabelDs.keyBy(data -> data.getString("uid"));
        KeyedStream<JSONObject, String> keyedStreamBase6LabelDs = mapBase6LabelDs.keyBy(data -> data.getString("uid"));

        // 对 keyedStreamBase2LabelDs 和 keyedStreamBase4LabelDs 进行区间连接操作，时间范围为前后 1 小时
        SingleOutputStreamOperator<JSONObject> processJoinBase2AndBase4LabelDs = keyedStreamBase2LabelDs.intervalJoin(keyedStreamBase4LabelDs)
                .between(Time.hours(-1), Time.hours(1))
                .process(new ProcessJoinBase2AndBase4Func());

        // 对连接后的结果按照 "uid" 字段进行分组
        KeyedStream<JSONObject, String> keyedStreamJoinBase2AndBase4LabelDs = processJoinBase2AndBase4LabelDs.keyBy(data -> data.getString("uid"));

        // 对 keyedStreamJoinBase2AndBase4LabelDs 和 keyedStreamBase6LabelDs 进行区间连接操作，时间范围为前后 1 小时
        SingleOutputStreamOperator<JSONObject> processUserLabelDs = keyedStreamJoinBase2AndBase4LabelDs.intervalJoin(keyedStreamBase6LabelDs)
                .between(Time.hours(-1), Time.hours(1))
                .process(new ProcessJoinBase6LabelFunc());

        // 将处理后的结果转换为 JSON 字符串，并写入 Kafka 主题 kafka_label_user_baseline_topic
        processUserLabelDs.map(data -> data.toJSONString())
                .sinkTo(
                        KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_label_user_baseline_topic)
                );

        // 打印处理后的结果，方便调试
        processUserLabelDs.print("processUserLabelDs -> ");

        // 执行 Flink 任务
        env.execute();
    }
}