package com.yt.spark.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * 配置管理组件
 *
 * Created by yangtong on 17/5/5.
 */
public class Configuration {

    private static Properties prop = new Properties();

    static {
        try {
            InputStream in = Configuration.class
                    .getClassLoader().getResourceAsStream("my.properties");
            //可以把key value对加载进去了
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 返回字符串类型
     * @param key
     * @return 字符串类型的value
     */
    public static String getProperty(String key) {
        return prop.getProperty(key);
    }


    /**
     * 返回布尔类型
     * @param key
     * @return 布尔类型的value
     */
    public static Boolean getBoolean(String key) {
        String value = getProperty(key);
        try {
            return Boolean.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 返回long类型
     * @param key
     * @return long类型的value
     */
    public static Long getLong(String key) {
        String value = getProperty(key);
        try {
            return Long.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0L;
    }

    /**
     * 返回int类型
     * @param key
     * @return
     */
    public static Integer getInteger(String key) {
        String value = getProperty(key);
        try {
            return Integer.valueOf(value);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }
}
