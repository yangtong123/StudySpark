package com.yt.spark.spark.product;

import com.alibaba.fastjson.JSONObject;
import com.yt.spark.conf.Configuration;
import com.yt.spark.conf.Constants;
import com.yt.spark.dao.IAreaTop3ProductDAO;
import com.yt.spark.dao.ITaskDAO;
import com.yt.spark.dao.factory.DAOFactory;
import com.yt.spark.domain.AreaTop3Product;
import com.yt.spark.domain.Task;
import com.yt.spark.util.ParamUtils;
import com.yt.spark.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * Created by yangtong on 17/5/18.
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args) {
        // 得到SparkSession
        SparkSession spark = SparkUtils.getSparkSession(Constants.SPARK_APP_NAME_PRODUCT);


//        spark.conf().set("spark.sql.shuffle.partitions", 100);

        // 注册自定义函数
        spark.udf().register("concat_long_string", new ConcatLongStringUDF(), DataTypes.StringType);
        spark.udf().register("group_concat_distinct", new GroupConcatDistinctUDAF());
        spark.udf().register("random_prefix", new RandomPrefixUDF(), DataTypes.StringType);
        spark.udf().register("remove_random_prefix", new RemoveRandomPrefixUDF(), DataTypes.StringType);
        spark.udf().register("get_json_object", new GetJsonObjectUDF(), DataTypes.StringType);

        // 准备模拟数据
        SparkUtils.mockData(spark);

        // 获取命令行传入的taskid，查询对应的任务参数
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PRODUCT);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskid);

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        String startDate = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE);

        // 查询用户指定日期范围内点击数据
        // 技术点1: hive数据源的使用
        JavaPairRDD<Long, Row> cityId2ClickActionRDD = getCityId2ClickActionRDDByDate(spark, startDate, endDate);
        System.out.println("cityId2ClickActionRDD : " + cityId2ClickActionRDD.count());

        // 从mysql中查询城市信息
        // 技术点2: 异构数据源之mysql的使用
        JavaPairRDD<Long, Row> cityId2CityInfoRDD = getCityId2CityInfoRDD(spark);
        System.out.println("cityId2CityInfoRDD : " + cityId2CityInfoRDD.count());

        // 生成点击商品基础信息临时表
        // 技术点3: 将RDD转化为DateFrame，并注册临时表
        generateTempClickProductBasicTable(spark, cityId2ClickActionRDD, cityId2CityInfoRDD);

        // 生成各区域各商品点击次数的临时表
        generateTempAreaProductClickTable(spark);

        // 生成包含完整商品信息的各区域各商品点击次数的临时表
        generateTempAreaFullProductClickCountTable(spark);

        // 使用开窗函数获取各个区域内点击次数排名前3的热门商品
        JavaRDD<Row> areaTop3ProductRDD = getAreaTop3ProductRDD(spark);

        // 因为数据量比较少，所以就先collect到本地，然后批量插入
        List<Row> rows = areaTop3ProductRDD.collect();
        System.out.println("rows: " + rows.size());
        persistAreaTop3Product(taskid, rows);

        spark.close();
    }

    /**
     * 查询用户指定日期范围内点击数据
     * @param spark
     * @param startDate
     * @param endDate
     * @return
     */
    private static JavaPairRDD<Long, Row> getCityId2ClickActionRDDByDate(SparkSession spark, String startDate, String endDate) {
        String sql =
                "SELECT "
                    + "city_id,"
                    + "click_product_id product_id "
                + "FROM user_visit_action "
                + "WHERE click_product_id IS NOT NULL "
                + "AND date >='" + startDate + "' "
                + "AND date <='" + endDate + "'";

        Dataset<Row> clickActionDF = spark.sql(sql);

        JavaRDD<Row> clickActionRDD = clickActionDF.toJavaRDD();
        JavaPairRDD<Long, Row> cityId2ClickActionRDD = clickActionRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Long cityId = row.getLong(0);
                        return new Tuple2<Long, Row>(cityId, row);
                    }
                });
        return cityId2ClickActionRDD;
    }

    /**
     * 使用spark SQL从mysql中查询城市信息
     * @param spark
     * @return
     */
    private static JavaPairRDD<Long, Row> getCityId2CityInfoRDD(SparkSession spark) {
        // 构建mysql连接配置信息（直接从配置环境中获取）
        String url = null;
        String user = null;
        String password = null;
        boolean local = Configuration.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            url = Configuration.getProperty(Constants.JDBC_URL);
            user = Configuration.getProperty(Constants.JDBC_USER);
            password = Configuration.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = Configuration.getProperty(Constants.JDBC_URL_PROD);
            user = Configuration.getProperty(Constants.JDBC_USER_PROD);
            password = Configuration.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        Map<String, String> options = new HashMap<>();
        options.put("url", url);
        options.put("dbtable", "city_info");
        options.put("user", user);
        options.put("password", password);

        // 使用Spark SQL从mysql中读取数据
        Dataset<Row> cityInfoDF = spark.read().format("jdbc")
                .options(options).load();

        JavaRDD<Row> cityInfoRDD = cityInfoDF.javaRDD();

        JavaPairRDD<Long, Row> cityId2CityInfoRDD = cityInfoRDD.mapToPair(
                new PairFunction<Row, Long, Row>() {
                    @Override
                    public Tuple2<Long, Row> call(Row row) throws Exception {
                        Object cityId = row.get(0);
                        return new Tuple2<Long, Row>(Long.valueOf(String.valueOf(cityId)), row);
                    }
                });

        return cityId2CityInfoRDD;
    }


    /**
     * 生成点击商品基础信息临时表
     * @param spark
     * @param cityId2ClickActionRDD
     * @param cityId2CityInfoRDD
     */
    private static void generateTempClickProductBasicTable(SparkSession spark,
                                                           JavaPairRDD<Long, Row> cityId2ClickActionRDD,
                                                           JavaPairRDD<Long, Row> cityId2CityInfoRDD) {
        // 进行join操作，进行点击行为数据和城市数据的关联
        JavaPairRDD<Long, Tuple2<Row, Row>> joinedRDD = cityId2ClickActionRDD.join(cityId2CityInfoRDD);

        // 先转化成JavaRDD<Row>，然后转化成DataFrame
        JavaRDD<Row> mappedRDD = joinedRDD.map(
                new Function<Tuple2<Long, Tuple2<Row, Row>>, Row>() {
                    @Override
                    public Row call(Tuple2<Long, Tuple2<Row, Row>> tuple2) throws Exception {
                        long cityId = tuple2._1;
                        Row clickAction = tuple2._2._1;
                        Row cityInfo = tuple2._2._2;

                        long productId = clickAction.getLong(1);
                        String cityName = cityInfo.getString(1);
                        String area = cityInfo.getString(2);

                        return RowFactory.create(cityId, cityName, area, productId);
                    }
                });
        List<StructField> structFieldList = new ArrayList<>();
        structFieldList.add(DataTypes.createStructField("city_id", DataTypes.LongType, true));
        structFieldList.add(DataTypes.createStructField("city_name", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("area", DataTypes.StringType, true));
        structFieldList.add(DataTypes.createStructField("product_id", DataTypes.LongType, true));

        StructType schema = DataTypes.createStructType(structFieldList);

        Dataset<Row> df = spark.createDataFrame(mappedRDD,schema);

        // 注册成临时表
        df.registerTempTable("tmp_click_product_basic");
    }


    /**
     * 生成各区域各商品点击次数临时表
     * @param spark
     */
    private static void generateTempAreaProductClickTable(SparkSession spark) {
        // 计算各区域各商品点击次数
        // 可以取到每个area下的每个product_id的城市信息拼接起来的串
        String sql =
                "SELECT " +
                    "area," +
                    "product_id," +
                    "count(*) click_count, " +
                    "group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos " +
                "FROM tmp_click_product_basic " +
                "GROUP BY area, product_id";

        /**
         * 双重group
         */
        /*String _sql =
                "SELECT " +
                    "product_id_area, " +
                    "count(click_count) click_count, " +
                    "group_concat_distinct(city_infos) city_infos " +
                "FROM (" +
                    "SELECT " +
                        "remove_random_prefix(product_id_area) product_id_area, " +
                        "click_count, " +
                        "city_infos " +
                    "FROM (" +
                        "SELECT " +
                            "product_id_area, " +
                            "count(*) click_count, " +
                            "group_concat_distinct(concat_long_string(city_id, city_name, ':')) city_infos" +
                        "FROM (" +
                            "SELECT" +
                                "random_prefix(concat_long_string(product_id, area, ':'), 10) product_id_area, " +
                                "city_id, " +
                                "city_name " +
                            "FROM tmp_click_product_basic " +
                        ") t1" +
                        "GROUP BY product_id_area " +
                    ") t2" +
                ") t3" +
                "GROUP BY product_id_area";*/

        Dataset df = spark.sql(sql);

        // 再次将查询出来的数据注册成一个临时表
        df.registerTempTable("tmp_area_product_click_count");
    }


    /**
     * 生成区域商品点击次数临时表(包含了商品的完整信息)
     * @param spark
     */
    private static void generateTempAreaFullProductClickCountTable(SparkSession spark) {
        String sql =
                "SELECT "
                    + "tapcc.area,"
                    + "tapcc.product_id,"
                    + "tapcc.click_count,"
                    + "tapcc.city_infos,"
                    + "pi.product_name,"
                    + "if(get_json_object(extend_info, 'product_status')=0, '自营', '第三方') product_status "
                + "FROM tmp_area_product_click_count tapcc "
                + "JOIN product_info pi ON tapcc.product_id=pi.product_id ";

        /**
         * 随机key与扩容表
         */
        /*JavaRDD<Row> rdd = spark.sql("select * from product_info").javaRDD();
        JavaRDD<Row> flattedRDD = rdd.flatMap(new FlatMapFunction<Row, Row>() {
            @Override
            public Iterator<Row> call(Row row) throws Exception {
                List<Row> list = new ArrayList<Row>();

                for (int i = 0; i < 10; i++) {
                    long productId = row.getLong(Constants.PRODUCT_INFO_PRODUCT_ID);
                    String prefixProductId = i + "_" + productId;

                    Row _row = RowFactory.create(productId, row.get(Constants.PRODUCT_INFO_PRODUCT_NAME),
                            row.get(Constants.PRODUCT_INFO_EXTEND_INFO));
                    list.add(_row);
                }

                return list.iterator();
            }
        });

        StructType _schema = DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("product_id", DataTypes.StringType, true),
                DataTypes.createStructField("product_name", DataTypes.StringType, true),
                DataTypes.createStructField("product_info", DataTypes.StringType, true)));

        Dataset _df = spark.createDataFrame(flattedRDD, _schema);
        _df.registerTempTable("tmp_product_info");

        String _sql =
                "SELECT " +
                    "tapcc.area, " +
                    "remove_random_prefix(tapcc.product_id) product_id, " +
                    "tapcc.click_count, " +
                    "tapcc.city_infos, " +
                    "pi.product_name, " +
                    "if(get_json_object(pi.extend_info, 'product_status')=0, '自营商品', '第三方商品') product_status" +
                "FROM (" +
                    "SELECT " +
                        "area, " +
                        "random_prefix(product_id, 10) product_id, " +
                        "click_count, " +
                        "city_infos" +
                    "FROM tmp_area_product_click_count " +
                ") tapcc " +
                "JOIN tmp_product_info pi ON tapcc.product_id = pi.product_id ";*/

        Dataset df = spark.sql(sql);

        df.registerTempTable("tmp_area_full_product_click_count");
    }

    /**
     * 获取各区域top3热门商品
     * @param spark
     * @return
     */
    private static JavaRDD<Row> getAreaTop3ProductRDD(SparkSession spark) {
       String sql =
               "SELECT "
                   + "area, "
                   + "CASE "
                       + "WHEN area='华北' OR area='华东' THEN 'A Level' "
                       + "WHEN area='华南' OR area='华中' THEN 'B Level' "
                       + "WHEN area='西北' OR area='西南' THEN 'C Level' "
                       + "ELSE 'D Level' "
                   + "END area_level, "
                   + "product_id, "
                   + "click_count, "
                   + "city_infos, "
                   + "product_name, "
                   + "product_status "
               + "FROM ("
                   + "SELECT "
                       + "area, "
                       + "product_id, "
                       + "click_count, "
                       + "city_infos, "
                       + "product_name, "
                       + "product_status, "
                       + "ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC) rank "
                   + "FROM tmp_area_full_product_click_count"
               + ") t "
               + "WHERE rank<=3";

        Dataset df = spark.sql(sql);

        System.out.println("getAreaTop3ProductRDD : ");
//        df.show();

        return df.toJavaRDD();
    }


    /**
     *
     * @param taskId
     * @param rows
     */
    private static void persistAreaTop3Product(Long taskId, List<Row> rows) {
        List<AreaTop3Product> areaTop3Products = new ArrayList<>();

        for (Row row : rows) {
            AreaTop3Product areaTop3Product = new AreaTop3Product();
            areaTop3Product.setTaskId(taskId);
            areaTop3Product.setArea(row.getString(0));
            areaTop3Product.setAreaLevel(row.getString(1));
            areaTop3Product.setProductId(row.getLong(2));
            areaTop3Product.setClickCount(Long.valueOf(String.valueOf(row.get(3))));
            areaTop3Product.setCityInfos(row.getString(4));
            areaTop3Product.setProductName(row.getString(5));
            areaTop3Product.setProductStatus(row.getString(6));

            areaTop3Products.add(areaTop3Product);
        }

        IAreaTop3ProductDAO areaTop3ProductDAO = DAOFactory.getAreaTop3ProductDAO();
        areaTop3ProductDAO.insertBatch(areaTop3Products);
    }
}
