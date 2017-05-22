package com.yt.spark.spark.product;

import org.apache.spark.sql.api.java.UDF1;

/**
 * Created by yangtong on 17/5/21.
 */
public class RemoveRandomPrefixUDF implements UDF1<String, String> {
    @Override
    public String call(String s) throws Exception {
        return s.split("_")[1];
    }
}
