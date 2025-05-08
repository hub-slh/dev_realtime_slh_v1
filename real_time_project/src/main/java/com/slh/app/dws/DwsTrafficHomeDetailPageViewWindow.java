package com.slh.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.slh.bean.TrafficHomeDetailPageViewBean;
import com.slh.function.BeanToJsonStrMapFunction;
import com.slh.utils.DateFormatUtil;
import com.slh.utils.FlinkSinkUtil;
import com.slh.utils.FlinkSourceUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
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
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @Package com.slh.app.dws.DwsTrafficHomeDetailPageViewWindow
 * @Author lihao_song
 * @Date 2025/4/18 15:03
 * @description: 流量域首页和详情页独立访客统计窗口
 * 该程序用于统计首页和商品详情页的独立访客数(UV)，并将结果写入Doris数据库。
 */
public class DwsTrafficHomeDetailPageViewWindow {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置并行度为1
        env.setParallelism(1);

        // 启用检查点，每5000毫秒做一次检查点，保证Exactly-Once语义
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        // 从Kafka获取数据源，主题为dwd_traffic_page，消费者组为dws_traffic_home_detail_page_view_window
        KafkaSource<String> kafkaSource = FlinkSourceUtil.getKafkaSource(
                "dwd_traffic_page",
                "dws_traffic_home_detail_page_view_window"
        );

        // 创建Kafka数据流
        DataStreamSource<String> kafkaStrDS = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "Kafka_Source"
        );

        // 1.将JSON字符串转换为JSONObject
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.map(JSON::parseObject);

        // 2.过滤出首页和详情页的访问记录
        SingleOutputStreamOperator<JSONObject> filterDS = jsonObjDS.filter(
                new FilterFunction<JSONObject>() {
                    @Override
                    public boolean filter(JSONObject jsonObj) {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        return "home".equals(pageId) || "good_detail".equals(pageId);
                    }
                }
        );

        // 3.指定Watermark生成策略和事件时间字段
        SingleOutputStreamOperator<JSONObject> withWatermarkDS = filterDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()  // 单调递增的水位线策略
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        // 从JSON对象中提取时间戳
                                        return jsonObj.getLong("ts");
                                    }
                                }
                        )
        );

        // 4.按照设备ID(mid)进行分组
        KeyedStream<JSONObject, String> keyedDS = withWatermarkDS.keyBy(
                jsonObj -> jsonObj.getJSONObject("common").getString("mid")
        );

        // 5.使用状态编程判断是否为独立访客，并统计UV
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> beanDS = keyedDS.process(
                new KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>() {
                    private ValueState<String> homeLastVisitDateState;  // 记录设备上次访问首页的日期
                    private ValueState<String> detailLastVisitDateState; // 记录设备上次访问详情页的日期

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化状态变量，设置TTL为1天
                        ValueStateDescriptor<String> homeValueStateDescriptor =
                                new ValueStateDescriptor<>("homeLastVisitDateState", String.class);
                        homeValueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1)).build()
                        );
                        homeLastVisitDateState = getRuntimeContext().getState(homeValueStateDescriptor);

                        ValueStateDescriptor<String> detailValueStateDescriptor =
                                new ValueStateDescriptor<>("detailLastVisitDateState", String.class);
                        detailValueStateDescriptor.enableTimeToLive(
                                StateTtlConfig.newBuilder(Time.days(1)).build()
                        );
                        detailLastVisitDateState = getRuntimeContext().getState(detailValueStateDescriptor);
                    }

                    @Override
                    public void processElement(
                            JSONObject jsonObj,
                            KeyedProcessFunction<String, JSONObject, TrafficHomeDetailPageViewBean>.Context ctx,
                            Collector<TrafficHomeDetailPageViewBean> out
                    ) throws Exception {
                        String pageId = jsonObj.getJSONObject("page").getString("page_id");
                        Long ts = jsonObj.getLong("ts");
                        String curVisitDate = DateFormatUtil.tsToDate(ts);
                        long homeUvCt = 0L;
                        long detailUvCt = 0L;

                        if ("home".equals(pageId)) {
                            // 首页UV统计：如果当天首次访问则计数
                            String homeLastVisitDate = homeLastVisitDateState.value();
                            if (StringUtils.isEmpty(homeLastVisitDate) || !homeLastVisitDate.equals(curVisitDate)) {
                                homeUvCt = 1L;
                                homeLastVisitDateState.update(curVisitDate);
                            }
                        } else {
                            // 详情页UV统计：如果当天首次访问则计数
                            String detailLastVisitDate = detailLastVisitDateState.value();
                            if (StringUtils.isEmpty(detailLastVisitDate) || !detailLastVisitDate.equals(curVisitDate)) {
                                detailUvCt = 1L;
                                detailLastVisitDateState.update(curVisitDate);
                            }
                        }

                        // 如果当天首次访问首页或详情页，则输出统计结果
                        if (homeUvCt != 0L || detailUvCt != 0L) {
                            out.collect(new TrafficHomeDetailPageViewBean(
                                    "", "", "", homeUvCt, detailUvCt, ts
                            ));
                        }
                    }
                }
        );

        // 6.定义1秒的滚动事件时间窗口
        AllWindowedStream<TrafficHomeDetailPageViewBean, TimeWindow> windowDS = beanDS
                .windowAll(TumblingEventTimeWindows.of(
                        org.apache.flink.streaming.api.windowing.time.Time.seconds(1)
                ));

        // 7.窗口聚合：统计首页和详情页的UV数
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> reduceDS = windowDS.reduce(
                new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(
                            TrafficHomeDetailPageViewBean value1,
                            TrafficHomeDetailPageViewBean value2
                    ) {
                        // 累加首页和详情页的UV数
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void process(
                            ProcessAllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>.Context context,
                            Iterable<TrafficHomeDetailPageViewBean> elements,
                            Collector<TrafficHomeDetailPageViewBean> out
                    ) {
                        // 处理窗口结果，添加时间信息
                        TrafficHomeDetailPageViewBean viewBean = elements.iterator().next();
                        TimeWindow window = context.window();
                        String stt = DateFormatUtil.tsToDateTime(window.getStart());
                        String edt = DateFormatUtil.tsToDateTime(window.getEnd());
                        String curDate = DateFormatUtil.tsToDate(window.getStart());
                        viewBean.setStt(stt);
                        viewBean.setEdt(edt);
                        viewBean.setCurDate(curDate);
                        out.collect(viewBean);
                    }
                }
        );

        // 将结果对象转换为JSON字符串
        SingleOutputStreamOperator<String> jsonMap = reduceDS.map(
                new BeanToJsonStrMapFunction<>()
        );

        // 打印结果（调试用）
        jsonMap.print();

        // 将结果写入Doris数据库
        jsonMap.sinkTo(
                FlinkSinkUtil.getDorisSink("dws_traffic_home_detail_page_view_window")
        );

        // 执行任务
        env.execute("DwsTrafficHomeDetailPageViewWindow");
    }
}