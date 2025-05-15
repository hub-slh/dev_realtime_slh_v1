package com.slh.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;

/**
 * @Package com.slh.function.IntervalDbOrderInfoJoinOrderDetailProcessFunc
 * @Author song.lihao
 * @Date 2025/5/15 15:47
 * @description:
 */
public class IntervalDbOrderInfoJoinOrderDetailProcessFunc extends ProcessJoinFunction<JSONObject,JSONObject,JSONObject> {
    @Override
    public void processElement(JSONObject jsonObject1, JSONObject jsonObject2, ProcessJoinFunction<JSONObject, JSONObject, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
        JSONObject result = new JSONObject();
        result.putAll(jsonObject1);

        result.put("sku_num", Double.valueOf(jsonObject2.getString("sku_num")).longValue());
        result.put("split_coupon_amount", jsonObject2.getString("sku_num"));
        result.put("sku_name", jsonObject2.getString("sku_name"));
        result.put("order_price", jsonObject2.getString("order_price"));
        result.put("detail_id", jsonObject2.getString("id"));
        result.put("order_id", jsonObject2.getString("order_id"));

        result.put("sku_id", Double.valueOf(jsonObject2.getString("sku_id")).longValue());
        result.put("split_activity_amount", Double.valueOf(jsonObject2.getString("split_activity_amount")).longValue());
        result.put("split_total_amount", Double.valueOf(jsonObject2.getString("split_total_amount")).longValue());

        collector.collect(result);
    }
}
