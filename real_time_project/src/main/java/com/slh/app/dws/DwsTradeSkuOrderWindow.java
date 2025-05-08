package com.slh.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.bean.TradeSkuOrderBean;
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
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.math.BigDecimal;

/**
 * @Package com.slh.app.dws.DwsTradeSkuOrderWindow
 * @Author lihao_song
 * @Date 2025/4/18 13:52
 * @description: 交易域SKU粒度订单统计窗口
 * 该程序用于统计各SKU的订单金额、优惠金额等信息，并补充商品分类和品牌信息，最终将结果写入Doris数据库。
 */
public class DwsTradeSkuOrderWindow {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1
        env.setParallelism(1);

        // 启用检查点，每5000毫秒做一次检查点，保证Exactly-Once语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 设置重启策略，固定延迟重启3次，每次间隔3000毫秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 3000L));

        // 从Kafka获取数据源，主题为dwd_trade_order_detail，消费者组为dws_trade_sku_order_window
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_order_detail", "dws_trade_sku_order_window");

        // 创建Kafka数据流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");

        // 将JSON字符串转换为JSONObject，过滤空值
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) {
                        if (value != null) {
                            JSONObject jsonObj = JSON.parseObject(value);
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
                        // 初始化状态变量
                        ValueStateDescriptor<JSONObject> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastJsonObjState", JSONObject.class);
                        lastJsonObjState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        JSONObject lastJsonObj = lastJsonObjState.value();
                        if (lastJsonObj == null) {
                            // 如果是第一条记录，保存状态并注册5秒后的定时器
                            lastJsonObjState.update(jsonObj);
                            long currentProcessingTime = ctx.timerService().currentProcessingTime();
                            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + 5000L);
                        } else {
                            // 比较时间戳，保留最新的记录
                            String lastTs = lastJsonObj.getString("ts_ms");
                            String curTs = jsonObj.getString("ts_ms");
                            if (curTs.compareTo(lastTs) >= 0) {
                                lastJsonObjState.update(jsonObj);
                            }
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, JSONObject, JSONObject>.OnTimerContext ctx, Collector<JSONObject> out) throws Exception {
                        // 定时器触发时，输出状态中的数据并清除状态
                        JSONObject jsonObj = lastJsonObjState.value();
                        if (jsonObj != null) {
                            out.collect(jsonObj);
                            lastJsonObjState.clear();
                        }
                    }
                }
        );

        // 分配时间戳和水位线，用于处理时间窗口
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

        // 将JSON对象转换为TradeSkuOrderBean对象
        SingleOutputStreamOperator<TradeSkuOrderBean> beanDS = withWatermarkDS.map(
                new MapFunction<JSONObject, TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean map(JSONObject jsonObj) {
                        // 提取SKU相关金额信息
                        String skuId = jsonObj.getString("sku_id");
                        BigDecimal splitOriginalAmount = jsonObj.getBigDecimal("split_original_amount");
                        BigDecimal splitCouponAmount = jsonObj.getBigDecimal("split_coupon_amount");
                        BigDecimal splitActivityAmount = jsonObj.getBigDecimal("split_activity_amount");
                        BigDecimal splitTotalAmount = jsonObj.getBigDecimal("split_total_amount");
                        Long ts = jsonObj.getLong("ts_ms") * 1000;
                        return TradeSkuOrderBean.builder()
                                .skuId(skuId)
                                .originalAmount(splitOriginalAmount)
                                .couponReduceAmount(splitCouponAmount)
                                .activityReduceAmount(splitActivityAmount)
                                .orderAmount(splitTotalAmount)
                                .ts_ms(ts)
                                .build();
                    }
                }
        );

        // 按照SKU ID分组
        KeyedStream<TradeSkuOrderBean, String> skuIdKeyedDS = beanDS.keyBy(TradeSkuOrderBean::getSkuId);

        // 定义10秒的处理时间滚动窗口
        WindowedStream<TradeSkuOrderBean, String, TimeWindow> windowDS = skuIdKeyedDS
                .window(TumblingProcessingTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)));

        // 窗口聚合：计算每个SKU的订单金额和优惠金额
        SingleOutputStreamOperator<TradeSkuOrderBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TradeSkuOrderBean>() {
                    @Override
                    public TradeSkuOrderBean reduce(TradeSkuOrderBean value1, TradeSkuOrderBean value2) {
                        // 累加各项金额
                        value1.setOriginalAmount(value1.getOriginalAmount().add(value2.getOriginalAmount()));
                        value1.setActivityReduceAmount(value1.getActivityReduceAmount().add(value2.getActivityReduceAmount()));
                        value1.setCouponReduceAmount(value1.getCouponReduceAmount().add(value2.getCouponReduceAmount()));
                        value1.setOrderAmount(value1.getOrderAmount().add(value2.getOrderAmount()));
                        return value1;
                    }
                },
                new ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<TradeSkuOrderBean, TradeSkuOrderBean, String, TimeWindow>.Context context,
                                        Iterable<TradeSkuOrderBean> elements, Collector<TradeSkuOrderBean> out) {
                        // 处理窗口结果，添加时间信息
                        TradeSkuOrderBean orderBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        orderBean.setStt(stt);
                        orderBean.setEdt(edt);
                        orderBean.setCurDate(curDate);
                        out.collect(orderBean);
                    }
                }
        );

        // 补充SPU信息（从HBase中查询）
        SingleOutputStreamOperator<TradeSkuOrderBean> withSpuInfoDS = reduceDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            if (orderBean.getSkuId() != null) {
                                // 从HBase中查询SKU信息
                                JSONObject skuInfoJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_sku_info", orderBean.getSkuId(), JSONObject.class);
                                if (skuInfoJsonObj != null) {
                                    orderBean.setSpuId(skuInfoJsonObj.getString("spu_id"));
                                    orderBean.setSpuName(skuInfoJsonObj.getString("spu_name"));
                                    orderBean.setTrademarkId(skuInfoJsonObj.getString("tm_id"));
                                    orderBean.setCategory3Id(skuInfoJsonObj.getString("category3_id"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting sku info for skuId: " + orderBean.getSkuId() + ", " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

        // 补充品牌信息
        SingleOutputStreamOperator<TradeSkuOrderBean> withTmDS = withSpuInfoDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            String tmId = orderBean.getTrademarkId();
                            if (tmId != null) {
                                // 从HBase中查询品牌信息
                                JSONObject tmJsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_base_trademark", tmId, JSONObject.class);
                                if (tmJsonObj != null) {
                                    orderBean.setTrademarkName(tmJsonObj.getString("tm_name"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting trademark info: " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

        // 补充三级分类信息
        SingleOutputStreamOperator<TradeSkuOrderBean> c3Stream = withTmDS.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            String category3Id = orderBean.getCategory3Id();
                            if (category3Id != null) {
                                // 从HBase中查询三级分类信息
                                JSONObject c3JsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_base_category3", category3Id, JSONObject.class);
                                if (c3JsonObj != null) {
                                    orderBean.setCategory3Name(c3JsonObj.getString("name"));
                                    orderBean.setCategory2Id(c3JsonObj.getString("category2_id"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting category3 info: " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

        // 补充二级分类信息
        SingleOutputStreamOperator<TradeSkuOrderBean> c2Stream = c3Stream.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            String category2Id = orderBean.getCategory2Id();
                            if (category2Id != null) {
                                // 从HBase中查询二级分类信息
                                JSONObject c2JsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_base_category2", category2Id, JSONObject.class);
                                if (c2JsonObj != null) {
                                    orderBean.setCategory2Name(c2JsonObj.getString("name"));
                                    orderBean.setCategory1Id(c2JsonObj.getString("category1_id"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting category2 info: " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

        // 补充一级分类信息
        SingleOutputStreamOperator<TradeSkuOrderBean> c1Stream = c2Stream.map(
                new RichMapFunction<TradeSkuOrderBean, TradeSkuOrderBean>() {
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
                    public TradeSkuOrderBean map(TradeSkuOrderBean orderBean) throws Exception {
                        try {
                            String category1Id = orderBean.getCategory1Id();
                            if (category1Id != null) {
                                // 从HBase中查询一级分类信息
                                JSONObject c1JsonObj = HBaseUtil.getRow(hbaseConn, Constant.HBASE_NAMESPACE,
                                        "dim_base_category1", category1Id, JSONObject.class);
                                if (c1JsonObj != null) {
                                    orderBean.setCategory1Name(c1JsonObj.getString("name"));
                                }
                            }
                        } catch (Exception e) {
                            System.err.println("Error getting category1 info: " + e.getMessage());
                        }
                        return orderBean;
                    }
                }
        );

        // 将结果对象转换为JSON字符串
        SingleOutputStreamOperator<String> jsonOrder = c1Stream.map(new BeanToJsonStrMapFunction<>());

        // 打印结果（调试用）
        jsonOrder.print();

        // 将结果写入Doris数据库
        jsonOrder.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_sku_order_window"));

        // 执行任务
        env.execute("DwsTradeSkuOrderWindow");
    }
}