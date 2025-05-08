package com.slh.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.bean.CartAddUuBean;
import com.slh.function.BeanToJsonStrMapFunction;
import com.slh.utils.DateFormatUtil;
import com.slh.utils.FlinkSinkUtil;
import com.slh.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.slh.app.dws.DwsTradeCartAddUuWindow
 * @Author lihao_song
 * @Date 2025/5/3 14:46
 * @description: DwsTradeCartAddUuWindow 交易购物车添加Uu窗口
 * 该程序用于统计每天首次将商品加入购物车的独立用户数，并将结果写入Doris数据库
 */

public class DwsTradeCartAddUuWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));
        // 从 kafka 获取数据源，主题为 dwd_trade_cart_add 消费者组为 dws_trade_cart_add_window
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource("dwd_trade_cart_add", "dws_trade_cart_add_uu_window");
        //创建 kafka 数据源
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka_Source");
        // 将 JSON 转化为 JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        SingleOutputStreamOperator<JSONObject> withWatermarkDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts_ms") * 1000;
                                    }
                                }
                        )
        );
        // 按照用户id进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(jsonObj -> jsonObj.getString("user_id"));
        // 处理每个用户数据 确保每天只统计一次加购行为
        SingleOutputStreamOperator<JSONObject> cartUUDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, JSONObject>() {
                     // 记录用户上次加购日期
                    private ValueState<String> lastCartDateState;

                    @Override
                    public void open(Configuration parameters) {
                        // 设置 TTL 为 1
                        ValueStateDescriptor<String> valueStateDescriptor
                                = new ValueStateDescriptor<>("lastCartDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                        lastCartDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, KeyedProcessFunction<String, JSONObject, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                        //从状态中获取上次加购日期
                        String lastCartDate = lastCartDateState.value();
                        //获取当前这次加购日期
                        Long ts = jsonObj.getLong("ts_ms");
                        String curCartDate = DateFormatUtil.tsToDate(ts);
                        // 如果上次加购日期为空或与当前日期不符合 则输出当前记录的更新状态
                        if (StringUtils.isEmpty(lastCartDate) || !lastCartDate.equals(curCartDate)) {
                            out.collect(jsonObj);
                            lastCartDateState.update(curCartDate);
                        }
                    }
                }
        );
        // 定义1秒的滚动时间时间窗口
        AllWindowedStream<JSONObject, TimeWindow> windowDS = cartUUDS
                .windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(1)));
        // 聚合计算窗口的独立用户数
        SingleOutputStreamOperator<CartAddUuBean> aggregateDS = windowDS.aggregate(
                new AggregateFunction<JSONObject, Long, Long>() {
                    // 初始化变量
                    @Override
                    public Long createAccumulator() {
                        return 0L;
                    }
                    // 每处理一条记录，累加器+1
                    @Override
                    public Long add(JSONObject value, Long accumulator) {
                        return ++accumulator;
                    }
                    // 返回累加器结果
                    @Override
                    public Long getResult(Long accumulator) {
                        return accumulator;
                    }

                    @Override
                    public Long merge(Long a, Long b) {
                        return null;
                    }
                },
                new AllWindowFunction<Long, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<Long> values, Collector<CartAddUuBean> out) {
                        // 获取窗口内的独立用户数
                        Long cartUUCt = values.iterator().next();
                        // 计算时间戳
                        long startTs = window.getStart() / 1000;
                        long endTs = window.getEnd() / 1000;
                        // 格式化时间
                        String stt = DateFormatUtil.tsToDateTime(startTs);
                        String edt = DateFormatUtil.tsToDateTime(endTs);
                        String curDate = DateFormatUtil.tsToDate(startTs);
                        // 输出结果
                        out.collect(new CartAddUuBean(
                                stt,
                                edt,
                                curDate,
                                cartUUCt
                        ));
                    }
                }
        );
        // 转化成JSON字符创
        SingleOutputStreamOperator<String> operator = aggregateDS
                .map(new BeanToJsonStrMapFunction<>());

//        operator.print();

        operator.sinkTo(FlinkSinkUtil.getDorisSink("dws_trade_cart_add_uu_window"));

        env.execute("DwsTradeCartAddUuWindow");
    }
}
