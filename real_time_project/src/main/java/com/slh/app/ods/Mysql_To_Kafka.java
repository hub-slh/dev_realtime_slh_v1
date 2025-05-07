package com.slh.app.ods;


import com.slh.utils.FlinkSinkUtil;
import com.slh.utils.FlinkSourceUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Package com.slh.app.ods.Mysql_To_Kafka
 * @Author lihao_song
 * @Date 2025/4/23 15:30
 * @description: MySQL数据实时同步到Kafka的Flink作业
 * 主要功能：
 *  * 1. 从MySQL数据库(dev_realtime_v1)捕获变更数据
 *  * 2. 将变更数据实时同步到Kafka的topic_db主题
 *  * 3. 支持全量和增量数据同步
 *  *
 *  * 注意事项：
 *  * 1. 并行度设置为2，可根据集群资源调整
 *  * 2. 使用了Flink CDC连接器捕获MySQL变更
 *  * 3. 当前未设置水位线策略(WatermarkStrategy.noWatermarks())
 */
public class Mysql_To_Kafka {
    public static void main(String[] args) throws Exception {
        // 创建流模式
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 检查点设置并行度为2
        env.setParallelism(2);
        // mysql中的数据库的名字以及要读取的表的名称
        MySqlSource<String> realtimeV1 = FlinkSourceUtil.getMySqlSource("dev_realtime_v1", "*");
        // 创建数据源，不设置水位线策略
        DataStreamSource<String> mySQLSource = env.fromSource(realtimeV1, WatermarkStrategy.noWatermarks(), "Mysql Source");
        // 输出读取的数据
        mySQLSource.print();
        // 设置存入卡芙卡的主题的名称
        KafkaSink<String> topicDb = FlinkSinkUtil.getKafkaSink("topic_db");
        // 将数据写入卡芙卡
        mySQLSource.sinkTo(topicDb);
        // 执行任务
        env.execute();
    }
}
