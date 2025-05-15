package com.slh.function;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.RichMapFunction;

/**
 * @Package com.slh.function.MapOrderDetailFunc
 * @Author song.lihao
 * @Date 2025/5/15 15:47
 * @description:
 */
public class MapOrderDetailFunc  extends RichMapFunction<JSONObject,JSONObject> {
    @Override
    public JSONObject map(JSONObject jsonObject) throws Exception {
        JSONObject result = new JSONObject();
        if (jsonObject.containsKey("after")){
            JSONObject after = jsonObject.getJSONObject("after");
            result.put("ts_ms",jsonObject.getLongValue("ts_ms"));
            result.put("sku_num",after.getLongValue("sku_num"));
            result.put("create_time",after.getLongValue("create_time"));
            result.put("sku_id",after.getLongValue("sku_id"));
            result.put("split_coupon_amount",after.getString("split_coupon_amount"));
            result.put("sku_name",after.getString("sku_name"));
            result.put("order_price",after.getString("order_price"));
            result.put("id",after.getString("id"));
            result.put("order_id",after.getString("order_id"));
            result.put("split_activity_amount",after.getString("split_activity_amount"));
            result.put("split_total_amount",after.getString("split_total_amount"));
        }
        return result;
    }
}

