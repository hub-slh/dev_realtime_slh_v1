package com.slh.Type.Data_Transmission;

import com.slh.utils.FlinkSinkUtil;
import com.slh.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.slh.app.ods.MySQL_To_Kafka
 * @Author song.lihao
 * @Date 2025/5/12 15:39
 * @description: 将 MySQL 中的 dev_realtime_v1 库中的所有表的数据 同步到 Kafka 中的 topic_db_v1 中
 * 主要功能：
 * 1. 从MySQL数据库(dev_realtime_v1)捕获变更数据
 * 2. 将变更数据实时同步到Kafka的topic_db主题
 * 3. 支持全量和增量数据同步
 * 注意事项：
 * 1. 并行度设置为2，可根据集群资源调整
 * 2. 使用了Flink CDC连接器捕获MySQL变更
 * 3. 当前未设置水位线策略(WatermarkStrategy.noWatermarks())
 */
public class MySQL_To_Kafka {
    public static void main(String[] args) throws Exception{
        // 创建流模型
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为2
        env.setParallelism(2);
        // MySQL中数据库中的名字以及要读取的表的名称
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("dev_realtime_v1", "*");
        // 创建数据源，但不设置水位线
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "MySQL Source");
        // 输出读取到的数据
        mySQLSource.print();
        // 设置 Kafka 主题名称
        KafkaSink<String> topicDb = FlinkSinkUtil.getKafkaSink("topic_db_v1");
        // 将数据写入到 Kafka 中
        mySQLSource.sinkTo(topicDb);
        // 执行任务
        env.execute();
    }
}
