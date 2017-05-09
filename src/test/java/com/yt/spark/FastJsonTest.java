package com.yt.spark;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * FastJson测试
 * Created by yangtong on 17/5/8.
 */
public class FastJsonTest {
    public static void main(String[] args) {
        String json = "[{'姓名': '张三', '年龄': '23'}, {'姓名': '李四', '年龄': '24'}]";

        JSONArray jsonArray = JSON.parseArray(json);
        JSONObject jsonObject = jsonArray.getJSONObject(0);
        System.out.println(jsonObject.getString("姓名"));
    }
}
