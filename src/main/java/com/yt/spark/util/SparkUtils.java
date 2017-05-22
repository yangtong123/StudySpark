package com.yt.spark.util;

import com.alibaba.fastjson.JSONObject;
import com.yt.spark.conf.Configuration;
import com.yt.spark.conf.Constants;
import com.yt.spark.spark.MockData;
import com.yt.spark.spark.session.CategorySortKey;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * spark工具类
 * Created by yangtong on 17/5/18.
 */
public class SparkUtils {
    /**
     * 根据当前是否本地测试的配置
     * 决定master
     */
    public static SparkSession getSparkSession(String appName) {
        boolean local = Configuration.getBoolean(Constants.SPARK_LOCAL);
        SparkSession spark = null;
        SparkConf conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{CategorySortKey.class}); //这个key在shuffle的时候，进行网络传输，因此设置为kyro序列化

        if (local) {
            spark = SparkSession
                    .builder()
                    .master("local")
                    .appName(appName)
                    .enableHiveSupport()
                    .getOrCreate();
        } else {
            spark = SparkSession.builder()
                    .appName(appName)
                    .config(conf)
                    .enableHiveSupport()
                    .getOrCreate();
        }

        return spark;
    }


    /**
     * 生成模拟数据(只有本地模式才会生成模拟数据)
     * @param spark
     */
    public static void mockData(SparkSession spark) {
        boolean local = Configuration.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mockData(spark);
        }
    }

    /**
     * 根据起始日期得到ActionRDD
     * @param spark
     * @param taskParams
     * @return
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SparkSession spark, JSONObject taskParams) {

        String startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE);

        String sql = "select * "
                + "from user_visit_action "
                + "where date >='" + startDate + "' "
                + "and date <='" + endDate + "'"
//                + "and session_id not in ('','','')"   //这个是用来过滤掉可能会导致数据倾斜的数据
                ;

        Dataset<Row> df = spark.sql(sql);

        /**
         * spark SQL的第一个stage的partition很少，所以使用repartition来重分区
         */
//        return df.javaRDD().repartition(1000);

        return df.javaRDD();
    }
}
