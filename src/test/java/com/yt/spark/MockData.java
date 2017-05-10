package com.yt.spark;

import com.yt.spark.util.*;
import com.yt.spark.util.StringUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.*;


/**
 * 模拟测试数据
 *
 * Created by yangtong on 17/5/8.
 */
public class MockData {
    public static void mockData(SparkSession spark) {
        List<Row> rows = new ArrayList<Row>();

        String[] searchKeywords = new String[] {"火锅", "蛋糕", "重庆辣子鸡", "重庆小面",
                "呷哺呷哺", "新辣道鱼火锅", "国贸大厦", "太古商场", "日本料理", "温泉"};
        String date = DateUtils.getTodayDate();
        String[] actions = new String[] {"search", "click", "order", "pay"};
        Random random = new Random();

        for (int i = 0; i < 100; i++) {
            long userid = random.nextInt(100);
            for (int j = 0; j < 10; j++) {
                String sessionid = UUID.randomUUID().toString().replace("-", "");
                String baseActionTime = date + " " + random.nextInt(23);

                Long clickCategoryId = null;
                for (int k = 0; k < random.nextInt(100); k++) {
                    long pageid = random.nextInt(10);
                    String actionTime = baseActionTime + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)))
                            + ":" + StringUtils.fulfuill(String.valueOf(random.nextInt(59)));
                    String searchKeyword = null;
                    Long clickProductId = null;
                    String orderCategoryIds = null;
                    String orderProductIds = null;
                    String payCategoryIds = null;
                    String payProductIds = null;

                    String action = actions[random.nextInt(4)];

                    switch (action) {
                        case "search" :
                            searchKeyword = searchKeywords[random.nextInt(10)];
                            break;
                        case "click" :
                            if (clickCategoryId == null) {
                                clickCategoryId = Long.valueOf(String.valueOf(random.nextInt(100)));
                            }
                            clickProductId = Long.valueOf(String.valueOf(random.nextInt(100)));
                            break;
                        case "order" :
                            orderCategoryIds = String.valueOf(random.nextInt(100));
                            orderProductIds = String.valueOf(random.nextInt(100));
                            break;
                        case "pay" :
                            payCategoryIds = String.valueOf(random.nextInt(100));
                            payProductIds = String.valueOf(random.nextInt(100));
                            break;
                    }

                    Row row = RowFactory.create(date, userid, sessionid, pageid, actionTime, searchKeyword,
                            clickCategoryId, clickProductId, orderCategoryIds, orderProductIds,
                            payCategoryIds, payProductIds, Long.valueOf(String.valueOf(random.nextInt(10))));

                    rows.add(row);
                }
            }
        }



        StructType schema = DataTypes.createStructType(Arrays.asList(
           DataTypes.createStructField("date", DataTypes.StringType, true),
           DataTypes.createStructField("user_id", DataTypes.LongType, true),
           DataTypes.createStructField("session_id", DataTypes.StringType, true),
           DataTypes.createStructField("page_id", DataTypes.LongType, true),
           DataTypes.createStructField("action_time", DataTypes.StringType, true),
           DataTypes.createStructField("search_keyword", DataTypes.StringType, true),
           DataTypes.createStructField("click_category_id", DataTypes.LongType, true),
           DataTypes.createStructField("click_product_id", DataTypes.LongType, true),
           DataTypes.createStructField("order_category_ids", DataTypes.StringType, true),
           DataTypes.createStructField("order_product_ids", DataTypes.StringType, true),
           DataTypes.createStructField("pay_category_ids", DataTypes.StringType, true),
           DataTypes.createStructField("pay_product_ids", DataTypes.StringType, true),
           DataTypes.createStructField("city_id", DataTypes.LongType, true)));

        Dataset<Row> df = spark.createDataFrame(rows, schema);

        df.createOrReplaceTempView("user_visit_action");
        df.takeAsList(1).forEach(System.out::println);

        /**
         * ======================================
         */
        rows.clear();
        String[] sexes = new String[] {"male", "female"};
        for (int i = 0; i < 100; i++) {
            long userid = i;
            String username = "user" + i;
            String name = "name" + i;
            int age = random.nextInt(60);
            String professional = "professional" + random.nextInt(100);
            String city = "city" + random.nextInt(100);
            String sex = sexes[random.nextInt(2)];

            Row row = RowFactory.create(userid, username, name, age, professional, city, sex);
            rows.add(row);
        }

        StructType schema2 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("user_id", DataTypes.LongType, true),
                DataTypes.createStructField("username", DataTypes.StringType, true),
                DataTypes.createStructField("name", DataTypes.StringType, true),
                DataTypes.createStructField("age", DataTypes.IntegerType, true),
                DataTypes.createStructField("professional", DataTypes.StringType, true),
                DataTypes.createStructField("city", DataTypes.StringType, true),
                DataTypes.createStructField("sex", DataTypes.StringType, true)));

        Dataset<Row> df2 = spark.createDataFrame(rows,schema2);

        df2.createOrReplaceTempView("user_info");
        df2.toDF().takeAsList(1).forEach(System.out::println);

        /**
         * ==========================================
         */
        rows.clear();

        int[] productStatus = new int[] {0, 1};

        for (int i = 0; i < 100; i++) {
            long productId = i;
            String productName = "product" + i;
            String extendInfo = "{\"product_status\":" +productStatus[random.nextInt(2)] + "}";

            Row row = RowFactory.create(productId, productName, extendInfo);
            rows.add(row);
        }

        StructType schema3 = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.LongType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("extend_info", DataTypes.StringType, true)));

        Dataset<Row> df3 = spark.createDataFrame(rows, schema3);

        df3.createOrReplaceTempView("product_info");
        df3.toDF().takeAsList(1).forEach(System.out::println);

    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local[2]").appName("testMockData").getOrCreate();
        mockData(spark);
    }

}
