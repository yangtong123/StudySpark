package com.yt.spark.spark;

import com.alibaba.fastjson.JSONObject;
import com.yt.spark.conf.Configuration;
import com.yt.spark.conf.Constants;
import com.yt.spark.dao.ITaskDAO;
import com.yt.spark.dao.impl.DAOFactory;
import com.yt.spark.util.ParamUtils;
import com.yt.spark.util.StringUtils;
import com.yt.spark.util.ValidUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Iterator;

/**
 * 用户访问session分析
 *
 * 接收用户创建的分析任务，用户可能指定的条件如下：
 * 1: 时间范围：起始日期－结束日期
 * 2: 性别：男或女
 * 3: 年龄范围
 * 4: 职业：多选
 * 5: 城市：多选
 * 6: 搜索词：多个搜索词，只要某个session中的任何一个action搜索过指定的关键词，那么session就符合条件
 * 7: 点击品类：多个品类，只要某个session中的任何一个action点击过某个品类，那么session就符合条件
 *
 * Created by yangtong on 17/5/8.
 */
public class UserVisitSessionAnalyzeSpark {
    public static void main(String[] args) {

        SparkSession spark = getSparkSession();

        //生成模拟数据
        mockData(spark);

        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        //session粒度数据聚合, 首先从user_visit_action表中, 查询出来指定日期范围类的数据
        //首先得查询出来指定的任务
//        long taskid = ParamUtils.getTaskIdFromArgs(args);


        spark.close();
    }



    /**
     * 获取SparkSession
     * 生产环境支持hive表
     * @return
     */
    private static SparkSession getSparkSession() {
        boolean local = Configuration.getBoolean(Constants.SPARK_LOCAL);
        SparkSession spark = null;
        if (local) {
            spark = SparkSession.builder()
                    .appName(Constants.SPARK_APP_NAME_SESSION)
                    .master("local[2]")
                    .getOrCreate();
        } else {
            spark = SparkSession.builder()
                    .appName(Constants.SPARK_APP_NAME_SESSION)
                    .enableHiveSupport()
                    .getOrCreate();
        }
        return spark;
    }

    /**
     * 生成模拟数据(只有本地模式才会生成模拟数据)
     * @param spark
     */
    private static void mockData(SparkSession spark) {
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
    private static JavaRDD<Row> getActionRDDByDateRange(SparkSession spark, JSONObject taskParams) {

        String startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE);

        String sql = "select * " +
                "from user_visit_action " +
                "where date >='" + startDate + "' " +
                "and date <='" + endDate + "'";

        Dataset<Row> df = spark.sql(sql);

        return df.javaRDD();
    }

    /**
     * 对行为数据按session粒度进行聚合
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, String> aggregateBySession(JavaRDD<Row> actionRDD, SparkSession spark) {

        JavaPairRDD<String, Row> sessionid2ActionRDD = actionRDD.mapToPair(
                /**
                 * PairFunction
                 * 第一个参数，相当于是函数的输入
                 * 第二个参数和第三个参数，相当于是函数的输出(Tuple), 分别是Tuple第一个和第二个值
                 *
                 * lambda
                 * (PairFunction<Row, String, Row>) row -> new Tuple2<String, Row>(row.getString(2), row));
                 *
                 */
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(2), row);
                    }
                });

        //对行为数据按session粒度进行分组
        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        //对每一个session分组进行聚合，将session中所有的搜索词和点击品类都聚合起来
        //得到<userid, partAggrInfo>
        JavaPairRDD<Long, String> userid2PartAggrInfoRDD = sessionid2ActionsRDD.mapToPair(
                new PairFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<String, Iterable<Row>> tuple) throws Exception {
                        String sessionid = tuple._1;
                        Iterator<Row> iterator = tuple._2.iterator();

                        StringBuffer searchKeywordsBuffer = new StringBuffer("");
                        StringBuffer clickCategoryIdBuffer = new StringBuffer("");

                        Long userid = null;

                        //遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            //提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(1);
                            }
                            String searchKeyword = row.getString(5);
                            Long clickCategoryId = row.getLong(6);

                            //只有搜索行为，只有searchKeyword字段的
                            //只有点击品类，只有clickCategoryId字段的
                            //所以任何一个行为数据，都不可能两个字段都有

                            //我们决定是否将搜索词或点击品类id拼接到字符串中
                            //首先要满足：不能是null值
                            //其次：之前的字符串中还没有搜索词或者点击品类id
                            if (StringUtils.isNotEmpty(searchKeyword)) {
                                if (!searchKeywordsBuffer.toString().contains(searchKeyword)) {
                                    searchKeywordsBuffer.append(searchKeyword + ",");
                                }
                            }
                            if (clickCategoryId != null) {
                                if (!clickCategoryIdBuffer.toString().contains(String.valueOf(clickCategoryId))) {
                                    clickCategoryIdBuffer.append(clickCategoryId + ",");
                                }
                            }
                        }

                        String searchKeyWords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdBuffer.toString());

                        //还需要跟用户信息进行聚合，所以key值应该是userid


                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|" +
                                Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|" +
                                Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|";

                        return new Tuple2<Long, String>(userid, partAggrInfo);
                    }
                });

        //查询所有用户数据，并得到<userid, Row>格式
        String sql = "select * from user_info";

        JavaRDD<Row> userInfoRDD = spark.sql(sql).javaRDD();

        JavaPairRDD<Long, Row> userid2InfoRDD = userInfoRDD.mapToPair(new PairFunction<Row, Long, Row>() {
            @Override
            public Tuple2<Long, Row> call(Row row) throws Exception {
                return new Tuple2<Long, Row>(row.getLong(0), row);
            }
        });

        //将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD = userid2PartAggrInfoRDD.join(userid2InfoRDD);

        //对join起来的数据进行拼接
        JavaPairRDD<String, String> sessionid2FullInfoRDD = userid2FullInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<String, Row>> tuple) throws Exception {
                        String partAggrInfo = tuple._2._1;
                        Row userInfoRow = tuple._2._2;

                        String sessionid = StringUtils.getFieldFromConcatString(partAggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                        int age = userInfoRow.getInt(3);
                        String professional = userInfoRow.getString(4);
                        String city = userInfoRow.getString(5);
                        String sex = userInfoRow.getString(6);

                        String fullAggrInfo = partAggrInfo + "|" +
                                Constants.FIELD_AGE + "=" + age + "|" +
                                Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
                                Constants.FIELD_CITY + "=" + city + "|" +
                                Constants.FIELD_SEX + "+" + sex + "|";

                        return new Tuple2<String, String>(sessionid, fullAggrInfo);
                    }
                });

        return sessionid2FullInfoRDD;
    }


    private static JavaPairRDD<String, String> filterSession(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD, final JSONObject taskParam) {

        String startAge = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE);
        String endAge = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE);
        String professional = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS);
        String cities = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES);
        String sex = ParamUtils.getParam(taskParam, Constants.PARAM_SEX);
        String keywords = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS);
        String categoryIds = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS);

        String _parameter = (startAge != null ? Constants.PARAM_START_AGE + "=" + startAge + "|" : "") +
                (endAge != null ? Constants.PARAM_END_AGE + "=" + endAge + "|" : "") +
                (professional != null ? Constants.PARAM_PROFESSIONALS + "=" + professional + "|" : "") +
                (cities != null ? Constants.PARAM_CITIES + "=" + cities + "|" : "") +
                (sex != null ? Constants.PARAM_SEX + "=" + sex + "|" : "") +
                (keywords != null ? Constants.PARAM_KEYWORDS + "=" + keywords + "|" : "") +
                (categoryIds != null ? Constants.PARAM_CATEGORY_IDS + "=" + categoryIds + "|" : "");

        if (_parameter.endsWith("\\|")) {
            _parameter = _parameter.substring(0, _parameter.length() - 1);
        }

        final String parameter = _parameter;



        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD = sessionid2AggrInfoRDD.filter(
                new Function<Tuple2<String, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, String> tuple) throws Exception {
                        String aggrInfo = tuple._2;

                        //按照年龄范围进行过滤
                        if (!ValidUtils.between(aggrInfo, Constants.FIELD_AGE, parameter,
                                Constants.PARAM_START_AGE, Constants.PARAM_END_AGE)) {
                            return false;
                        }

                        //按照职业范围进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_PROFESSIONAL, parameter,
                                Constants.PARAM_PROFESSIONALS)) {
                            return false;
                        }

                        //按照城市范围进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CITY, parameter,
                                Constants.PARAM_CITIES)) {
                            return false;
                        }

                        //按照性别进行过滤
                        if (!ValidUtils.equal(aggrInfo, Constants.FIELD_SEX, parameter,
                                Constants.PARAM_SEX)) {
                            return false;
                        }

                        // 按照搜索词进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_SEARCH_KEYWORDS, parameter,
                                Constants.PARAM_KEYWORDS)) {
                            return false;
                        }

                        //按照点击品类id进行过滤
                        if (!ValidUtils.in(aggrInfo, Constants.FIELD_CLICK_CATEGORY_IDS, parameter,
                                Constants.PARAM_CATEGORY_IDS)) {
                            return false;
                        }

                        return true;
                    }
                });

        return null;
    }
}
