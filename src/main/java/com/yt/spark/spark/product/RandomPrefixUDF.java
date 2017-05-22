package com.yt.spark.spark.product;

import org.apache.spark.sql.api.java.UDF2;

import java.util.Random;

/**
 * Created by yangtong on 17/5/21.
 */
public class RandomPrefixUDF implements UDF2<String, Integer, String> {
    @Override
    public String call(String str, Integer num) throws Exception {
        Random random = new Random();
        return random.nextInt(num) + "_" + str;
    }
}
