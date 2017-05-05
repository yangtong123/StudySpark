package com.yt.spark.conf;


/**
 * Created by yangtong on 17/5/5.
 */
public class ConfigurationTest {
    public static void main(String[] args) {
        String testkey1 = Configuration.getProperty("yt2");
        System.out.println(testkey1);
    }
}