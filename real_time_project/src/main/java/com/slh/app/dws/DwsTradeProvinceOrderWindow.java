package com.slh.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.bean.TradeProvinceOrderBean;
import com.slh.constant.Constant;
import com.slh.function.BeanToJsonStrMapFunction;
import com.slh.utils.DateFormatUtil;
import com.slh.utils.FlinkSinkUtil;
import com.slh.utils.FlinkSourceUtil;
import com.slh.utils.HBaseUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;

/**
 * @Package com.slh.app.dws.DwsTradeProvinceOrderWindow
 * @Author lihao_song
 * @Date 2025/5/0 14:47
 * @description: 交易域省份订单窗口统计
 * 该程序用于统计各省份的订单数量和订单金额，并将结果写入Doris数据库。
 */
public class DwsTradeProvinceOrderWindow {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1
        env.setParallelism(1);

        // 启用检查点，每5000毫秒做一次检查点，保证Exactly-Once语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略，固定延迟重启3次，每次间隔3000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 从Kafka获取数据源，主题为dwd_trade_order_detail，消费者组为dws_trade_province_order_window
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_order_detail", "dws_trade_province_order_window");

        // 创建Kafka数据流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 将JSON字符串转换为JSONObject，过滤空值
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        if (jsonStr != null) {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            out.collect(jsonObj);
                        }
                    }
                }
        );

        // 按照订单明细ID分组，用于去重
        KeyedStream<JSONObject, String> orderDetailIdKeyedDS = jsonObjDS.keyBy(jsonObj -> jsonObj.getString("id"));

        // 去重处理：对于同一订单明细ID，只保留最新的一条记录
        SingleOutputStreamOperator<JSONObject> distinctDS = orderDetailIdKeyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                    private ValueState<JSONObject> lastJsonObjState;  // 状态变量，记录上次处理的JSON对象
                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态变量，设置TTL为10秒
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)).build());
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj != null) {
                            // 如果存在旧记录，标记其split_total_amount为负值（用于后续处理）
                            String splitTotalAmount = lastJsonObj.getString("split_total_amount");
                            lastJsonObj.put("split_total_amount", "-" + splitTotalAmount);
                            out.collect(lastJsonObj);
                        }
                        // 更新状态并输出当前记录
                        lastJsonObjState.update(jsonObj);
                        out.collect(jsonObj);
                    }
                }
        );

        // 分配时间戳和水位线，用于事件时间处理
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = distinctDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()  // 单调递增的水位线策略
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        // 从JSON对象中提取时间戳（单位为毫秒）
                                        return jsonObj.getLong("ts_ms") * 1000;
                                    }
                                }
                        )
        );

        // 将JSON对象转换为TradeProvinceOrderBean对象
        SingleOutputStreamOperator<TradeProvinceOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean map(JSONObject jsonObj) {
                        String provinceId = jsonObj.getString("province_id");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts_ms");
                        String orderId = jsonObj.getString("order_id");
                        return TradeProvinceOrderBean.builder()
                                .provinceId(provinceId)
                                .orderAmount(splitTotalAmount)
                                .orderIdSet(new HashSet<>(Collections.singleton(orderId)))
                                .ts(ts)
                                .build();
                    }
                }
        );

        // 按照省份ID分组
        KeyedStream<TradeProvinceOrderBean, String> provinceIdKeyedDS = beanDS.keyBy(TradeProvinceOrderBean::getProvinceId);

        // 定义1秒的滚动事件时间窗口
        WindowedStream<TradeProvinceOrderBean, String, TimeWindow> windowDS = provinceIdKeyedDS
                .window(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1)));

        // 窗口聚合：计算每个省份的订单总金额和订单数
        SingleOutputStreamOperator<TradeProvinceOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeProvinceOrderBean>() {
                    @Override
                    public TradeProvinceOrderBean reduce(TradeProvinceOrderBean value1, TradeProvinceOrderBean value2) {
                        // 累加订单金额和订单ID集合
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        value1.getOrderIdSet().addAll(value2.getOrderIdSet());
                        return value1;
                    }
                },
                new WindowFunction<TradeProvinceOrderBean, TradeProvinceOrderBean, String, TimeWindow>() {
                    @Override
                    public void apply(String s, TimeWindow window, Iterable<TradeProvinceOrderBean> input, Collector<TradeProvinceOrderBean> out) {
                        // 处理空窗口情况
                        if (!input.iterator().hasNext()) {
                            TradeProvinceOrderBean emptyBean = new TradeProvinceOrderBean();
                            emptyBean.setStt(DateFormatUtil.tsToDateTime(window.getStart()));
                            emptyBean.setEdt(DateFormatUtil.tsToDateTime(window.getEnd()));
                            emptyBean.setCurDate(DateFormatUtil.tsToDate(window.getStart()));
                            emptyBean.setOrderAmount(BigDecimal.ZERO);
                            emptyBean.setOrderCount(0L);
                            out.collect(emptyBean);
                            return;
                        }
                        // 处理非空窗口
                        TradeProvinceOrderBean orderBean = input.iterator().next();
                        long startTs = window.getStart() / 1000;
                        long endTs = window.getEnd() / 1000;
                        String stt = DateFormatUtil.tsToDateTime(startTs);
                        String edt = DateFormatUtil.tsToDateTime(endTs);
                        String curDate = DateFormatUtil.tsToDate(startTs);
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        orderBean.setOrderCount((long) orderBean.getOrderIdSet().size());
                        out.collect(orderBean);
                    }
                }
        );

        // 补充省份名称信息（从HBase中查询）
        SingleOutputStreamOperator<TradeProvinceOrderBean> withProvinceDS = reduceDS.map(
                new RichMapFunction<TradeProvinceOrderBean, TradeProvinceOrderBean>() {
                    private Connection hbaseConn;  // HBase连接

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HBaseUtil.getHBaseConnection();
                    }

                    @Override
                    public void close() throws Exception {
                        HBaseUtil.closeHBaseConnection(hbaseConn);
                    }

                    @Override
                    public TradeProvinceOrderBean map(TradeProvinceOrderBean orderBean) throws Exception {
                        String provinceId = orderBean.getProvinceId();
                        // 从HBase中查询省份名称
                        JSONObject provinceInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE, "dim_base_province", provinceId, JSONObject.class);
                        orderBean.setProvinceName(provinceInfoJsonObj.getString("name"));
                        return orderBean;
                    }
                }
        ).setParallelism(1);

        // 将结果对象转换为JSON字符串
        SingleOutputStreamOperator<String> sink = withProvinceDS.map(new BeanToJsonStrMapFunction<>());

        // 将结果写入Doris数据库
        sink.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_province_order_window"));

        // 执行任务
        env.execute("DwsTradeProvinceOrderWindow");
    }
}