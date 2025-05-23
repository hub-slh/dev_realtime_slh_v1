package com.slh.Type.Data_Transmission;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.bean.TableProcessDim;
import com.slh.constant.Constant;
import com.slh.function.HBaseSinkFunction;
import com.slh.function.TableProcessFunction;
import com.slh.utils.FlinkSourceUtil;
import com.slh.utils.HBaseUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @Package com.slh.app.dim.Kafka_To_Hbase
 * @Author song.lihao
 * @Date 2025/5/12 15:52
 * @description: 将 MySQL 中的维度表 上传到 Hbase 并创建维度表
 * 主要功能：
 * 1. 从Kafka的topic_db主题消费数据
 * 2. 过滤出dev_realtime_v1数据库的变更数据(c/u/d/r操作)
 * 3. 从MySQL配置表(table_process_dim)获取维度表处理规则
 * 4. 根据配置动态创建/删除HBase表
 * 5. 将处理后的维度数据写入HBase
 * 核心流程：
 * 1. Kafka数据源 -> 数据过滤 -> 配置表数据 -> 广播连接 -> 维度数据处理 -> HBase写入
 * 2. 使用Exactly-Once检查点机制保证数据一致性
 * 注意事项：
 * 1. 并行度设置为4，配置表处理使用单并行度
 * 2. 检查点间隔5000ms
 * 3. 使用广播状态共享维度表配置信息
 * 4. HBase表操作包括创建、删除和重建
 */
public class Kafka_To_Hbase1 {
    public static void main(String[] args) throws Exception {
        // 初始化流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度4
        env.setParallelism(4);
        // 启动检查点 5000ms 间隔 EXACTLY_ONCE 语义
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        // 从 MySQL 消费数据，消费组为dim_app
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(Constant.TOPIC_DB, "dim_app");
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
//        kafkaStrDS.print();
        //数据过滤：只处理dev_realtime_v1数据库的变更数据(c/u/d/r操作)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String db = jsonObj.getJSONObject("source").getString("db");
                        String op = jsonObj.getString("op");
                        String data = jsonObj.getString("after");
                        if ("dev_realtime_v1".equals(db)
                                && ("c".equals(op)
                                || "u".equals(op)
                                || "d".equals(op)
                                || "r".equals(op))
                                && data != null
                                && data.length() > 2
                        ){
                            out.collect(jsonObj);
                        }
                    }
                }
        );
//                        jsonObjDS.print();
        // 从 MySql 配置表中获取维度表处理规则
        MySqlSource<String> mySqlSource = FlinkSourceUtil.getMySqlSource("dev_realtime_v1_config", "table_process_dim");

        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysql_source").setParallelism(1);
//        mysqlStrDS.print();
        // 解析配置表为 TableProcessDim 对象
        SingleOutputStreamOperator<TableProcessDim> tpDS = mysqlStrDS.map(
                new MapFunction<String, TableProcessDim>() {
                    @Override
                    public TableProcessDim map(String jsonStr) {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String op = jsonObj.getString("op");
                        TableProcessDim tableProcessDim = null;
                        if ("d".equals(op)){
                            tableProcessDim = jsonObj.getObject("before", TableProcessDim.class);
                        }else {
                            tableProcessDim = jsonObj.getObject("after", TableProcessDim.class);
                        }
                        tableProcessDim.setOp(op);
                        return tableProcessDim;
                    }
                }
        ).setParallelism(1);
//        tpDS.print();
        // 根据配置动态管理Hbase表(创建/删除/重建)
        tpDS.map(
                new RichMapFunction<TableProcessDim, TableProcessDim>() {

                    private Connection hbaseConn;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcessDim map(TableProcessDim tp) {
                        String op = tp.getOp();
                        String sinkTable = tp.getSinkTable();
                        String[] sinkFamilies = tp.getSinkFamily().split(",");
                        if("d".equals(op)){//删除
                            HBaseUtil.dropHBaseTable(hbaseConn, Constant.HBASE_NAMESPACE,sinkTable);
                        }else if("r".equals(op)||"c".equals(op)){//读取或创建操作
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }else{//更新操作
                            HBaseUtil.dropHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable);
                            HBaseUtil.createHBaseTable(hbaseConn,Constant.HBASE_NAMESPACE,sinkTable,sinkFamilies);
                        }
                        return tp;
                    }
                }
        ).setParallelism(1);

//         tpDS.print();
        // 创建广播状态描述符，用于共享维度表配置
        MapStateDescriptor<String, TableProcessDim> mapStateDescriptor =
                new MapStateDescriptor<String, TableProcessDim>("mapStateDescriptor",String.class, TableProcessDim.class);
        BroadcastStream<TableProcessDim> broadcastDS = tpDS.broadcast(mapStateDescriptor);
        // 连接主数据流和广播流
        BroadcastConnectedStream<JSONObject, TableProcessDim> connectDS = jsonObjDS.connect(broadcastDS);
        // 处理连接后的；流，应用维度配置表
        SingleOutputStreamOperator<Tuple2<JSONObject,TableProcessDim>> dimDS = connectDS
                .process(new TableProcessFunction(mapStateDescriptor));

//        dimDS.print();
        // 将处理后的数据写入Hbase
        dimDS.addSink(new HBaseSinkFunction());

        env.execute("dim");
    }
}
