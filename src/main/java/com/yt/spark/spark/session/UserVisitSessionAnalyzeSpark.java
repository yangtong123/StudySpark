package com.yt.spark.spark.session;

import com.alibaba.fastjson.JSONObject;
import com.yt.spark.conf.Configuration;
import com.yt.spark.conf.Constants;
import com.yt.spark.dao.*;
import com.yt.spark.dao.factory.DAOFactory;
import com.yt.spark.domain.*;
import com.yt.spark.spark.MockData;
import com.yt.spark.util.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

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
        args = new String[] {"2"};

        SparkSession spark = SparkUtils.getSparkSession(Constants.SPARK_APP_NAME_SESSION);

        //生成模拟数据
        SparkUtils.mockData(spark);
//        mockData(spark);

        //创建需要使用的DAO组件
        ITaskDAO taskDAO = DAOFactory.getTaskDAO();

        //session粒度数据聚合, 首先从user_visit_action表中, 查询出来指定日期范围类的数据
        //首先得查询出来指定的任务
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_SESSION);

        Task task = taskDAO.findById(taskid);

        if (task == null) {
            System.out.println(new Date() + "Cannot find task with id: " + taskid + " .");
            return;
        }

        JSONObject taskParams = JSONObject.parseObject(task.getTaskParam());

        System.out.println(taskParams.toJSONString());

        //根据时间范围得到actionRDD
//        JavaRDD<Row> actionRDD = getActionRDDByDateRange(spark, taskParams);
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(spark, taskParams);

        //从actionRDD中抽取sessionid，得到<sessionid, actionRDD>
        JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);

        //得到<sessionid, AggrInfo>
        JavaPairRDD<String, String> sessionid2AggrInfoRDD = aggregateBySession(actionRDD, spark);

        System.out.println(sessionid2AggrInfoRDD.count());

        for (Tuple2<String, String> tuple : sessionid2AggrInfoRDD.take(10)) {
            System.out.println(tuple._2);
        }


        //将自定义的accumulator注册到SparkContext中
        AccumulatorV2<String, String> sessionAggrStatAccumulator = new SessionAggrStatAccumulator();
        spark.sparkContext().register(sessionAggrStatAccumulator);

        //过滤
        JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD =
                filterSessionAndAggrStat(sessionid2AggrInfoRDD, taskParams, sessionAggrStatAccumulator);


        //按时间比例随机抽取
        randomExtractSession(task.getTaskId(), spark, sessionid2AggrInfoRDD, sessionid2ActionRDD);


        /**
         * 对于把Accumulator中的数据持久化到数据库中，
         * ！！！一定要有action操作，并且是放在前面
         *
         */
        System.out.println(filteredSessionid2AggrInfoRDD.count());
        calculateAndPersistAggrStat(sessionAggrStatAccumulator.value(), task.getTaskId());

        JavaPairRDD<String, Row> sessionid2DetailRDD = getSessionid2DetailRDD(
                filteredSessionid2AggrInfoRDD, sessionid2ActionRDD);
        sessionid2DetailRDD = sessionid2DetailRDD.persist(StorageLevel.MEMORY_ONLY());

        List<Tuple2<CategorySortKey, String>> top10CategoryList =
                getTop10Category(task.getTaskId(), sessionid2DetailRDD);

        getTop10Session(spark, task.getTaskId(), top10CategoryList, sessionid2DetailRDD);



        spark.close();
    }


    /**
     * 获取SparkSession
     * 生产环境支持hive表
     * @return
     */
    /*private static SparkSession getSparkSession() {
        boolean local = Configuration.getBoolean(Constants.SPARK_LOCAL);
        SparkSession spark = null;
        SparkConf conf = new SparkConf()
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .registerKryoClasses(new Class[]{CategorySortKey.class}); //这个key在shuffle的时候，进行网络传输，因此设置为kyro序列化

        if (local) {
            spark = SparkSession.builder()
                    .appName(Constants.SPARK_APP_NAME_SESSION)
                    .master("local")
                    .config(conf)
                    .getOrCreate();
        } else {
            spark = SparkSession.builder()
                    .appName(Constants.SPARK_APP_NAME_SESSION)
                    .config(conf)
                    .enableHiveSupport()
                    .getOrCreate();
        }
        return spark;
    }*/

    /**
     * 生成模拟数据(只有本地模式才会生成模拟数据)
     * @param spark
     */
/*    private static void mockData(SparkSession spark) {
        boolean local = Configuration.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mockData(spark);
        }
    }*/

    /**
     * 根据起始日期得到ActionRDD
     * @param spark
     * @param taskParams
     * @return
     */
/*    private static JavaRDD<Row> getActionRDDByDateRange(SparkSession spark, JSONObject taskParams) {

        String startDate = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE);

        String sql = "select * " +
                "from user_visit_action " +
                "where date >='" + startDate + "' " +
                "and date <='" + endDate + "'";

        Dataset<Row> df = spark.sql(sql);

        *//**
         * spark SQL的第一个stage的partition很少，所以使用repartition来重分区
         *//*
//        return df.javaRDD().repartition(1000);

        return df.javaRDD();
    }*/

    /**
     * 得到sessionid2ActionRDD
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
//        return actionRDD.mapToPair(
//                new PairFunction<Row, String, Row>() {
//                    @Override
//                    public Tuple2<String, Row> call(Row row) throws Exception {
//                        return new Tuple2<String, Row>(row.getString(Constants.USER_VISIT_ACTION_SESSION_ID), row);
//                    }
//                });

        return actionRDD.mapPartitionsToPair(
                new PairFlatMapFunction<Iterator<Row>, String, Row>() {
                    @Override
                    public Iterator<Tuple2<String, Row>> call(Iterator<Row> iterator) throws Exception {
                        List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();

                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            list.add(new Tuple2<>(row.getString(Constants.USER_VISIT_ACTION_SESSION_ID), row));
                        }

                        return list.iterator();
                    }
                });
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

                        //session的起始时间
                        Date startTime = null;
                        Date endTime = null;
                        //session的访问步长
                        int stepLength = 0;

                        //遍历session所有的访问行为
                        while (iterator.hasNext()) {
                            //提取每个访问行为的搜索词字段和点击品类字段
                            Row row = iterator.next();
                            if (userid == null) {
                                userid = row.getLong(Constants.USER_VISIT_ACTION_USER_ID);
                            }
                            String searchKeyword = null;
                            Long clickCategoryId = null;

                            //只有搜索行为，只有searchKeyword字段的
                            //只有点击品类，只有clickCategoryId字段的
                            //所以任何一个行为数据，都不可能两个字段都有
                            //如果没有的话会出异常
                            try {
                                searchKeyword = row.getString(Constants.USER_VISIT_ACTION_SEARCH_KEYWORD);
                                clickCategoryId = row.getLong(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID);
                            } catch (Exception e) {

                            }

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

                            //计算session开始和结束时间
                            Date actionTime = DateUtils.parseTime(row.getString(Constants.USER_VISIT_ACTION_ACTION_TIME));
                            if (startTime == null) {
                                startTime = actionTime;
                            }
                            if (endTime == null) {
                                endTime = actionTime;
                            }
                            if (actionTime.before(startTime)) {
                                startTime = actionTime;
                            }
                            if (actionTime.after(endTime)) {
                                endTime = actionTime;
                            }
                            //计算session访问步长
                            stepLength++;
                        }

                        String searchKeyWords = StringUtils.trimComma(searchKeywordsBuffer.toString());
                        String clickCategoryIds = StringUtils.trimComma(clickCategoryIdBuffer.toString());

                        //计算session访问时长
                        long visitLength = (endTime.getTime() - startTime.getTime()) / 1000;

                        String partAggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionid + "|"
                                + Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeyWords + "|"
                                + Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCategoryIds + "|"
                                + Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|"
                                + Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|"
                                + Constants.FIELD_START_TIME + "=" + startTime;

                        System.out.println("aggregateBySession startTime: " + startTime);

                        //还需要跟用户信息进行聚合，所以key值应该是userid
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

        /**
         * 这里的join就可以把reduce join转换为map join
         */
        //将session粒度聚合数据，与用户信息进行join
        JavaPairRDD<Long, Tuple2<String, Row>> userid2FullInfoRDD =
                userid2PartAggrInfoRDD.join(userid2InfoRDD);

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

        /**
         * reduce join 转为 map join
         */
        /*JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        List<Tuple2<Long, Row>> userInfos = userid2InfoRDD.collect();
        final Broadcast<List<Tuple2<Long, Row>>> userInfosBroadcast = sc.broadcast(userInfos);

        JavaPairRDD<String, String> tunned = userid2PartAggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, String> tuple2) throws Exception {
                        List<Tuple2<Long, Row>> userInfos = userInfosBroadcast.value();
                        Map<Long, Row> userInfoMap = new HashMap<Long, Row>();

                        for (Tuple2<Long, Row> userInfo : userInfos) {
                            userInfoMap.put(userInfo._1(), userInfo._2());
                        }

                        //得到当前用户相应信息
                        String partAggrInfo = tuple2._2;
                        Row userInfoRow = userInfoMap.get(tuple2._1());

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
                });*/

        /**
         * sample采样倾斜key单独进行join
         */
        /*JavaPairRDD<Long, String> sampledRDD = userid2PartAggrInfoRDD.sample(false, 0.1, 9);

        JavaPairRDD<Long, Long> mappedSampledRDD = sampledRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<Long, String> tuple2) throws Exception {
                        return new Tuple2<Long, Long>(tuple2._1, 1L);
                    }
                });

        JavaPairRDD<Long, Long> computedSampledRDD = mappedSampledRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        JavaPairRDD<Long, Long> reversedSampledRDD = computedSampledRDD.mapToPair(
                new PairFunction<Tuple2<Long, Long>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<Long, Long> tuple2) throws Exception {
                        return new Tuple2<Long, Long>(tuple2._2, tuple2._1);
                    }
                });

        final Long skewedUserid = reversedSampledRDD.sortByKey(false).take(1).get(0)._2;

        JavaPairRDD<Long, String> skewedRDD = userid2PartAggrInfoRDD.filter(
                new Function<Tuple2<Long, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, String> tuple2) throws Exception {
                        return tuple2._1.equals(skewedUserid);
                    }
                });

        JavaPairRDD<Long, String> commonRDD = userid2PartAggrInfoRDD.filter(
                new Function<Tuple2<Long, String>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, String> tuple2) throws Exception {
                        return !tuple2._1.equals(skewedUserid);
                    }
                });

        *//**
         * 下面两步操作是改良版
         *//*
        JavaPairRDD<String, Row> skewedUserid2InfoRDD = userid2InfoRDD.filter(
                new Function<Tuple2<Long, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Long, Row> tuple2) throws Exception {
                        return tuple2._1.equals(skewedUserid);
                    }
                })
                .flatMapToPair(new PairFlatMapFunction<Tuple2<Long, Row>, String, Row>() {
                    @Override
                    public Iterator<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple2) throws Exception {
                        Random random = new Random();
                        List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();

                        for (int i = 0; i < 100; i++) {
                            int prefix = random.nextInt(100);
                            list.add(new Tuple2<>(prefix + "_" + tuple2._1, tuple2._2));
                        }

                        return list.iterator();
                    }
                });

        JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD1 = skewedRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, String> tuple2) throws Exception {
                        Random random = new Random();
                        int prefix = random.nextInt(100);

                        return new Tuple2<String, String>(prefix + "_" + tuple2._1, tuple2._2);
                    }
                })
                .join(skewedUserid2InfoRDD)
                .mapToPair(
                        new PairFunction<Tuple2<String, Tuple2<String, Row>>, Long, Tuple2<String, Row>>() {
                            @Override
                            public Tuple2<Long, Tuple2<String, Row>> call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
                                Long userid = Long.valueOf(tuple2._1.split("_")[1]);
                                return new Tuple2<Long, Tuple2<String, Row>>(userid, tuple2._2);
                            }
                        });

        JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD2 = commonRDD.join(userid2InfoRDD);

        JavaPairRDD<Long, Tuple2<String, Row>> joinedRDD = joinedRDD1.union(joinedRDD2);

        JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
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
                });*/

        /**
         * 使用随机数和扩容表
         */
        /*JavaPairRDD<String, Row> expandedRDD = userid2InfoRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Row>, String, Row>() {
                    @Override
                    public Iterator<Tuple2<String, Row>> call(Tuple2<Long, Row> tuple2) throws Exception {
                        List<Tuple2<String, Row>> list = new ArrayList<Tuple2<String, Row>>();
                        for (int i = 0; i < 10; i++) {
                            list.add(new Tuple2<>(i + "_" + tuple2._1, tuple2._2));
                        }
                        return list.iterator();
                    }
                });

        JavaPairRDD<String, String> mappedUserid2PartAggrInfoRDD = userid2PartAggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<Long, String> tuple2) throws Exception {
                        Random random = new Random();
                        int prefix = random.nextInt(10);
                        return new Tuple2<String, String>(prefix + "_" + tuple2._1, tuple2._2);
                    }
                });

        JavaPairRDD<String, Tuple2<String, Row>> joinedRDD = mappedUserid2PartAggrInfoRDD.join(expandedRDD);

        JavaPairRDD<String, String> finalRDD = joinedRDD.mapToPair(
                new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Row>> tuple) throws Exception {
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
                });*/


        return sessionid2FullInfoRDD;
    }


    /**
     * 过滤session数据，并进行聚合统计
     *
     * @param sessionid2AggrInfoRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, String> filterSessionAndAggrStat(
            JavaPairRDD<String, String> sessionid2AggrInfoRDD,
            final JSONObject taskParam,
            final AccumulatorV2<String, String> sessionAggrStatAccumulator) {

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

                        //过滤完了之后要对session的访问时长和访问步长进行统计
                        sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);

                        //计算出session的访问时长和访问步长的范围，并进行相应的累加
                        long visitLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_VISIT_LENGTH));
                        long stepLength = Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_STEP_LENGTH));

                        calculateVisitLength(visitLength);
                        calculateStepLength(stepLength);

                        return true;
                    }

                    /**
                     * 对session的时间范围进行累加
                     * @param visitLength
                     */
                    private void calculateVisitLength(long visitLength) {
                        if (visitLength >= 1 && visitLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                        } else if (visitLength >= 4 && visitLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                        } else if (visitLength >= 7 && visitLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                        } else if (visitLength >= 10 && visitLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                        } else if (visitLength > 30 && visitLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                        } else if (visitLength > 60 && visitLength <= 180) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                        } else if (visitLength > 180 && visitLength <= 600) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                        } else if (visitLength > 600 && visitLength <= 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                        } else if (visitLength > 1800) {
                            sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                        }
                    }

                    /**
                     * 对session的步长进行累加
                     * @param stepLength
                     */
                    private void calculateStepLength(long stepLength) {
                        if (stepLength >= 1 && stepLength <= 3) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                        } else if (stepLength >= 4 && stepLength <= 6) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                        } else if (stepLength >= 7 && stepLength <= 9) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                        } else if (stepLength >= 10 && stepLength <= 30) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                        } else if (stepLength > 30 && stepLength <= 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                        } else if (stepLength > 60) {
                            sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                        }
                    }
                });

        return filteredSessionid2AggrInfoRDD;
    }


    /**
     * 随机抽取session数据
     * @param sessionid2AggrInfoRDD
     */
    private static void randomExtractSession(final long taskid,
                                             SparkSession spark,
                                             JavaPairRDD<String, String> sessionid2AggrInfoRDD,
                                             JavaPairRDD<String, Row> sessionid2ActionRDD) {
        //得到<yyyy-MM-dd_HH, aggrInfo>
        JavaPairRDD<String, String> time2AggrInfoRDD = sessionid2AggrInfoRDD.mapToPair(
                new PairFunction<Tuple2<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Tuple2<String, String> tuple2) throws Exception {
                        String aggrInfo = tuple2._2;
                        //System.out.println("randomExtractSession aggrInfo: " + aggrInfo);
                        String startTime = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME);
                        //System.out.println("randomExtractSession startTime" + startTime);
                        String dateHour = DateUtils.getDateHour(startTime);

                        return new Tuple2<String, String>(dateHour, aggrInfo);
                    }});

        //计算每天每小时的session数量
        Map<String, Long> countMap = time2AggrInfoRDD.countByKey();

        //按时间比例随机抽取,计算出每天每小时要抽取session的索引
        //将<yyyy-MM-dd_HH, count>转变为<yyyy-MM-dd, <MM, count>>
        Map<String, Map<String, Integer>> dateHourCountMap = new HashMap<String, Map<String, Integer>>();
        for (Map.Entry<String, Long> countEntry : countMap.entrySet()) {
            String dateHour = countEntry.getKey();
            String date = dateHour.split("_")[0];
            String hour = dateHour.split("_")[1];
            long count = countEntry.getValue();

            Map<String, Integer> hourCountMap = dateHourCountMap.get(date);
            if (hourCountMap == null) {
                hourCountMap = new HashMap<>();
                dateHourCountMap.put(date, hourCountMap);
            }
            hourCountMap.put(hour, (int) count);
        }
        //总共要抽取100个session，先按照天数，进行平分
        long extractNumberPerDay = 100 / dateHourCountMap.size();

        //<date, <hour, (3,5,20,102)>>
        Map<String, Map<String, List<Integer>>> dateHourExtractMap = new HashMap<String, Map<String, List<Integer>>>();
        Random random = new Random();

        for (Map.Entry<String, Map<String, Integer>> dateHourCountEntry : dateHourCountMap.entrySet()) {
            String date = dateHourCountEntry.getKey();
            Map<String, Integer> hourCountMap = dateHourCountEntry.getValue();

            Map<String, List<Integer>> hourExtractMap = dateHourExtractMap.get(date);
            if (hourExtractMap == null) {
                hourExtractMap = new HashMap<String, List<Integer>>();
                dateHourExtractMap.put(date, hourExtractMap);
            }

            //计算出这一天的session总数
            long sessionCount = 0L;
            for (long hourCount : hourCountMap.values()) {
                sessionCount += hourCount;
            }

            //遍历小时
            for (Map.Entry<String, Integer> hourCountEntry : hourCountMap.entrySet()) {
                String hour = hourCountEntry.getKey();
                long count = hourCountEntry.getValue();
                //计算每个小时的session的数量占据当天session数量的比例,然后得到每个小时要抽取的数量
                int hourExtractNumber = (int) (((double) count / (double) sessionCount) * extractNumberPerDay);
                if (hourExtractNumber > count) {
                    hourExtractNumber = (int) count;
                }

                //得到当前小时存放的随机索引List
                List<Integer> extractIndexList = hourExtractMap.get(hour);
                if (extractIndexList == null) {
                    extractIndexList = new ArrayList<Integer>();
                    hourExtractMap.put(hour, extractIndexList);
                }

                //随机生成索引
                for (int i = 0; i < hourExtractNumber; i++) {
                    int extractIndex = random.nextInt((int) count);
                    while (extractIndexList.contains(extractIndex)) {
                        extractIndex = random.nextInt((int) count);
                    }
                    extractIndexList.add(extractIndex);
                }

            }
        }

        /**
         * fastutil的使用
         *
         */
        Map<String, Map<String, IntList>> fastutilDateHourExtractMap =
                new HashMap<String, Map<String, IntList>>();

        for (Map.Entry<String, Map<String, List<Integer>>> dateHourExtractEntry :
                dateHourExtractMap.entrySet()) {
            String date = dateHourExtractEntry.getKey();

            Map<String, List<Integer>> hourExtractMap = dateHourExtractEntry.getValue();
            Map<String, IntList> fastutilHourExtractMap = new HashMap<>();

            for (Map.Entry<String, List<Integer>> hourExtractEntry :
                    hourExtractMap.entrySet()) {
                String hour = hourExtractEntry.getKey();
                List<Integer> extractList = hourExtractEntry.getValue();
                IntList fastutilExtractList = new IntArrayList();

                for (int i = 0; i < extractList.size(); i++) {
                    fastutilExtractList.add(extractList.get(i));
                }

                fastutilHourExtractMap.put(hour, fastutilExtractList);
            }

            fastutilDateHourExtractMap.put(date, fastutilHourExtractMap);
        }


        /**
         * 广播变量
         * 这个map有点大，为了避免每个task都拷贝一份这个map，就把它设置成广播变量
         */
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        final Broadcast<Map<String, Map<String, IntList>>> dateHourExtractMapBroadcast =
                javaSparkContext.broadcast(fastutilDateHourExtractMap);

        //根据随机索引进行抽取
        JavaPairRDD<String, Iterable<String>> time2session2RDD = time2AggrInfoRDD.groupByKey();

        JavaPairRDD<String, String> extractSessionidsRDD = time2session2RDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<String>>, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<String, Iterable<String>> tuple2) throws Exception {
                        //存放返回值的list，使用tuple的原因是方便join
                        List<Tuple2<String, String>> extractSessionids = new ArrayList<Tuple2<String, String>>();

                        String dateHour = tuple2._1;
                        String date = dateHour.split("_")[0];
                        String hour = dateHour.split("_")[1];
                        Iterator<String> iterator = tuple2._2.iterator();


                        /**
                         * 使用广播变量的时候，直接调用广播变量的value方法
                         */
                        Map<String, Map<String, IntList>> dateHourExtractMap =
                                dateHourExtractMapBroadcast.value();
                        List<Integer> extractIndexList = dateHourExtractMap.get(date).get(hour);
                        ISessionRandomExtractDAO sessionRandomExtractDAO = DAOFactory.getSessionRandomExtractDAO();

                        int index = 0;
                        while (iterator.hasNext()) {
                            String aggrInfo = iterator.next();
                            String sessionid = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID);

                            if (extractIndexList.contains(index)) {
                                SessionRandomExtract sessionRandomExtract = new SessionRandomExtract();

                                sessionRandomExtract.setTaskid(taskid);
                                sessionRandomExtract.setSessionid(sessionid);
                                sessionRandomExtract.setStartTime(
                                        StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_START_TIME));
                                sessionRandomExtract.setSearchKeywords(
                                        StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS));
                                sessionRandomExtract.setClickCategoryIds(
                                        StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS));

                                //插入mysql
                                sessionRandomExtractDAO.insert(sessionRandomExtract);

                                //将sessionid放入list
                                extractSessionids.add(new Tuple2<>(sessionid, sessionid));
                            }

                            index++;
                        }

                        //需要返回iterator
                        return extractSessionids.iterator();
                    }});

        /**
         * 第四步，获取抽取出来的session的明细数据
         */
        JavaPairRDD<String, Tuple2<String, Row>> extractSessionDetailRDD =
                extractSessionidsRDD.join(sessionid2ActionRDD);

        extractSessionDetailRDD.foreach(
                new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
                    @Override
                    public void call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
                        Row row = tuple2._2._2;

                        SessionDetail sessionDetail = new SessionDetail();
                        sessionDetail.setTaskid(taskid);
                        try {
                            sessionDetail.setUserid(row.getLong(Constants.USER_VISIT_ACTION_USER_ID));
                            sessionDetail.setSessionid(row.getString(Constants.USER_VISIT_ACTION_SESSION_ID));
                            sessionDetail.setPageid(row.getLong(Constants.USER_VISIT_ACTION_PAGE_ID));
                            sessionDetail.setActionTime(row.getString(Constants.USER_VISIT_ACTION_ACTION_TIME));
                            sessionDetail.setSearchKeywords(row.getString(Constants.USER_VISIT_ACTION_SEARCH_KEYWORD));
                            sessionDetail.setClickCategoryId(row.getLong(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID));
                            sessionDetail.setClickProductId(row.getLong(Constants.USER_VISIT_ACTION_CLICK_PRODUCT_ID));
                            sessionDetail.setOrderCategoryIds(row.getString(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS));
                            sessionDetail.setOrderProductIds(row.getString(Constants.USER_VISIT_ACTION_ORDER_PRODUCT_IDS));
                            sessionDetail.setPayCategoryIds(row.getString(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS));
                            sessionDetail.setPayProductIds(row.getString(Constants.USER_VISIT_ACTION_PAY_PRODUCT_IDS));
                        } catch (Exception e) {

                        }

                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                        sessionDetailDAO.insert(sessionDetail);
                    }
                });

    }


    /**
     * 计算占比并持久化到数据库中
     * @param value
     * @param taskid
     */
    private static void calculateAndPersistAggrStat(String value, long taskid) {

        long session_count = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.SESSION_COUNT));

        long visit_length_1s_3s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1s_3s));
        long visit_length_4s_6s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_4s_6s));
        long visit_length_7s_9s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_7s_9s));
        long visit_length_10s_30s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10s_30s));
        long visit_length_30s_60s = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30s_60s));
        long visit_length_1m_3m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_3m_10m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_1m_3m));
        long visit_length_10m_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_10m_30m));
        long visit_length_30m = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.TIME_PERIOD_30m));

        long step_length_1_3 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_1_3));
        long step_length_4_6 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_4_6));
        long step_length_7_9 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_7_9));
        long step_length_10_30 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_10_30));
        long step_length_30_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_30_60));
        long step_length_60 = Long.valueOf(StringUtils.getFieldFromConcatString(value, "\\|", Constants.STEP_PERIOD_60));

        double visit_length_1s_3s_ratio = NumberUtils.formatDouble(
                (double) visit_length_1s_3s / (double) session_count, 2);
        double visit_length_4s_6s_ratio = NumberUtils.formatDouble(
                (double) visit_length_4s_6s / (double) session_count, 2);
        double visit_length_7s_9s_ratio = NumberUtils.formatDouble(
                (double) visit_length_7s_9s / (double) session_count, 2);
        double visit_length_10s_30s_ratio = NumberUtils.formatDouble(
                (double) visit_length_10s_30s / (double) session_count, 2);
        double visit_length_30s_60s_ratio = NumberUtils.formatDouble(
                (double) visit_length_30s_60s / (double) session_count, 2);
        double visit_length_1m_3m_ratio = NumberUtils.formatDouble(
                (double) visit_length_1m_3m / (double) session_count, 2);
        double visit_length_3m_10m_ratio = NumberUtils.formatDouble(
                (double) visit_length_3m_10m  / (double) session_count, 2);
        double visit_length_10m_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_10m_30m / (double) session_count, 2);
        double visit_length_30m_ratio = NumberUtils.formatDouble(
                (double) visit_length_30m / (double) session_count ,2);

        double step_length_1_3_ratio = NumberUtils.formatDouble(
                (double) step_length_1_3 / (double) session_count, 2);
        double step_length_4_6_ratio = NumberUtils.formatDouble(
                (double) step_length_4_6 / (double) session_count, 2);
        double step_length_7_9_ratio = NumberUtils.formatDouble(
                (double) step_length_7_9 / (double) session_count, 2);
        double step_length_10_30_ratio = NumberUtils.formatDouble(
                (double) step_length_10_30 / (double) session_count, 2);
        double step_length_30_60_ratio = NumberUtils.formatDouble(
                (double) step_length_30_60 / (double) session_count, 2);
        double step_length_60_ratio = NumberUtils.formatDouble(
                (double) step_length_60 / (double) session_count, 2);

        //将统计结果封装称domain
        SessionAggrStat sessionAggrStat = new SessionAggrStat();
        sessionAggrStat.setTaskid(taskid);
        sessionAggrStat.setSession_count(session_count);
        sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
        sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
        sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
        sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
        sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
        sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
        sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
        sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
        sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);

        sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
        sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
        sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
        sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
        sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
        sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);

        //调用对应的DAO插入统计结果
        ISessionAggrStatDAO sessionAggrStatDAO = DAOFactory.getSessionAggrStatDAO();
        sessionAggrStatDAO.insert(sessionAggrStat);

    }


    /**
     * 得到符合条件的session的访问明细
     * @param filteredSessionid2AggrInfoRDD
     * @param sessionid2ActionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2DetailRDD(
            JavaPairRDD<String, String> filteredSessionid2AggrInfoRDD,
            JavaPairRDD<String, Row> sessionid2ActionRDD) {

        JavaPairRDD<String, Row> sessionid2DetailRDD = filteredSessionid2AggrInfoRDD.join(sessionid2ActionRDD)
                .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Row>>, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
                        return new Tuple2<String, Row>(tuple2._1, tuple2._2._2);
                    }
                });
        return sessionid2DetailRDD;
    }

    /**
     * 获取top10热门品类
     * @param taskid
     * @param sessionid2DetailRDD
     */
    private static List<Tuple2<CategorySortKey, String>> getTop10Category(
            final long taskid, JavaPairRDD<String, Row> sessionid2DetailRDD) {
        /**
         * 第一步：获取附和条件的session访问过的所有品类
         */

        //获取session访问过的所有品类id，也就是点击过、下单过、支付过
        JavaPairRDD<Long, Long> categoryidRDD = sessionid2DetailRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                        List<Tuple2<Long, Long>> categoryList = new ArrayList<Tuple2<Long, Long>>();

                        Row row = tuple2._2;

                        try {
                            Long clickCategoryId = row.getLong(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID);
                            if (clickCategoryId != null) {
                                categoryList.add(new Tuple2<>(clickCategoryId, clickCategoryId));
                            }

                            String orderCategoryIds = row.getString(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS);
                            if (orderCategoryIds != null) {
                                String[] orderCategoryIdSplited = orderCategoryIds.split(",");
                                for (String orderCategoryId : orderCategoryIdSplited) {
                                    categoryList.add(new Tuple2<>(Long.valueOf(orderCategoryId), Long.valueOf(orderCategoryId)));
                                }
                            }

                            String payCategoryIds = row.getString(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS);
                            if (payCategoryIds != null) {
                                String[] payCategoryIdSplited = payCategoryIds.split(",");
                                for (String payCategoryId : payCategoryIdSplited) {
                                    categoryList.add(new Tuple2<>(Long.valueOf(payCategoryId), Long.valueOf(payCategoryId)));
                                }
                            }

                        } catch (Exception e) {
                        }

                        return categoryList.iterator();
                    }
                });

        /**
         * 一定要进行去重
         */
        categoryidRDD = categoryidRDD.distinct();

        /**
         * 第二步：计算各品类的点击、下单和支付的次数
         */

        //
        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = getClickCategoryId2CountRDD(sessionid2DetailRDD);
        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = getOrderCategoryId2CountRDD(sessionid2DetailRDD);
        JavaPairRDD<Long, Long> payCategoryId2CountRDD = getPayCategoryId2CountRDD(sessionid2DetailRDD);

        /**
         * 第三步：join操作
         */
        JavaPairRDD<Long, String> categoryid2CountRDD = joinCategoryAndData(
                categoryidRDD, clickCategoryId2CountRDD, orderCategoryId2CountRDD, payCategoryId2CountRDD);

        /**
         * 第四步：自定义二次排序key
         */


        /**
         * 第五步：将数据映射成<soryKey, Info>格式的RD，然后进行二次排序
         */
        JavaPairRDD<CategorySortKey, String> sortKey2CountRDD = categoryid2CountRDD.mapToPair(
                new PairFunction<Tuple2<Long, String>, CategorySortKey, String>() {
                    @Override
                    public Tuple2<CategorySortKey, String> call(Tuple2<Long, String> tuple2) throws Exception {
                        String countInfo = tuple2._2;
                        long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
                        long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
                        long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                                countInfo, "\\|", Constants.FIELD_PAY_COUNT));

                        CategorySortKey sortKey = new CategorySortKey(clickCount, orderCount, payCount);

                        return new Tuple2<CategorySortKey, String>(sortKey, countInfo);
                    }
                });

        JavaPairRDD<CategorySortKey, String> sortedCategoryCountRDD = sortKey2CountRDD.sortByKey(false);

        /**
         * 第6步：用take(10)取出,并持久化到数据库中
         */
        List<Tuple2<CategorySortKey, String>> top10CategoryList = sortedCategoryCountRDD.take(10);
        ITop10CategoryDAO top10CategoryDAO = DAOFactory.getTop10CategoryDAO();
        for (Tuple2<CategorySortKey, String> tuple2 : top10CategoryList) {
            String countInfo = tuple2._2;
            long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CATEGORY_ID));
            long clickCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_CLICK_COUNT));
            long orderCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_ORDER_COUNT));
            long payCount = Long.valueOf(StringUtils.getFieldFromConcatString(
                    countInfo, "\\|", Constants.FIELD_PAY_COUNT));

            Top10Category category = new Top10Category();
            category.setTaskid(taskid);
            category.setCategoryid(categoryid);
            category.setClickCount(clickCount);
            category.setOrderCount(orderCount);
            category.setPayCount(payCount);

            top10CategoryDAO.insert(category);
        }

        return top10CategoryList;
    }

    /**
     * 获取各品类点击次数RDD
     * @param sessionid2DetailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getClickCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRDD) {

        //过滤
        /**
         * 因为过滤出来的点击行为很少，所以过滤后每个paitition中的数据量很不均匀，而且很少
         * 所以可以使用coalesce
         */
        JavaPairRDD<String, Row> clickActionRDD = sessionid2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
                        Row row = tuple2._2;
                        boolean flag = row.get(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID) != null ? true : false;
                        return flag;
                    }
                })
//                .coalesce(100)
                ;

        JavaPairRDD<Long, Long> clickCategoryIdRDD = clickActionRDD.mapToPair(
                new PairFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Row> tuple2) throws Exception {
                        long clickCategoryId = tuple2._2.getLong(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID);
                        return new Tuple2<Long, Long>(clickCategoryId, 1L);
                    }
                });


        JavaPairRDD<Long, Long> clickCategoryId2CountRDD = clickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });



        //给每个key打一个随机数
        JavaPairRDD<String, Long> mappedClickCategoryIdRDD = clickCategoryIdRDD.mapToPair(
                new PairFunction<Tuple2<Long, Long>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<Long, Long> tuple2) throws Exception {
                        Random random = new Random();
                        int prefix = random.nextInt(10);

                        return new Tuple2<String, Long>(prefix + "_" + tuple2._1, tuple2._2);
                    }
                });
        /**
         * 下面是为了防止数据倾斜做的优化
         */
        /*//执行第一轮聚合
        JavaPairRDD<String, Long> firstAggrRDD = mappedClickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });
        //去除每个key的前缀
        JavaPairRDD<Long, Long> restoredRDD = firstAggrRDD.mapToPair(
                new PairFunction<Tuple2<String, Long>, Long, Long>() {
                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, Long> tuple2) throws Exception {
                        Long categoryId = Long.valueOf(tuple2._1.split("_")[1]);
                        return new Tuple2<Long, Long>(categoryId, tuple2._2);
                    }
                });

        //第二轮全局整合
        JavaPairRDD<String, Long> globalAggrRDD = mappedClickCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });*/

        return clickCategoryId2CountRDD;
    }

    /**
     * 获取各品类下单次数RDD
     * @param sessionid2DetailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getOrderCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRDD) {

        //过滤
        JavaPairRDD<String, Row> orderActionRDD = sessionid2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
                        Row row = tuple2._2;
                        boolean flag = row.get(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS) != null ? true : false;
                        return flag;
                    }
                });

        JavaPairRDD<Long, Long> orderCategoryIdRDD = orderActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                        String orderCategoryIds = tuple2._2.getString(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS);
                        String[] orderCategoryIdsSplited = orderCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        for (String orderCategoryId : orderCategoryIdsSplited) {
                            list.add(new Tuple2<>(Long.valueOf(orderCategoryId), 1L));
                        }

                        return list.iterator();
                    }
                });



        JavaPairRDD<Long, Long> orderCategoryId2CountRDD = orderCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        return orderCategoryId2CountRDD;
    }

    /**
     * 获取各品类支付次数RDD
     * @param sessionid2DetailRDD
     * @return
     */
    private static JavaPairRDD<Long, Long> getPayCategoryId2CountRDD(JavaPairRDD<String, Row> sessionid2DetailRDD) {

        //过滤
        JavaPairRDD<String, Row> payActionRDD = sessionid2DetailRDD.filter(
                new Function<Tuple2<String, Row>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Row> tuple2) throws Exception {
                        Row row = tuple2._2;
                        boolean flag = row.get(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS) != null ? true : false;
                        return flag;
                    }
                });

        JavaPairRDD<Long, Long> payCategoryIdRDD = payActionRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Row>, Long, Long>() {
                    @Override
                    public Iterator<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple2) throws Exception {
                        String payCategoryIds = tuple2._2.getString(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS);
                        String[] payCategoryIdsSplited = payCategoryIds.split(",");

                        List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                        for (String payCategoryId : payCategoryIdsSplited) {
                            list.add(new Tuple2<>(Long.valueOf(payCategoryId), 1L));
                        }

                        return list.iterator();
                    }
                });



        JavaPairRDD<Long, Long> payCategoryId2CountRDD = payCategoryIdRDD.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        return payCategoryId2CountRDD;
    }

    private static JavaPairRDD<Long, String> joinCategoryAndData(
            JavaPairRDD<Long, Long> categoryidRDD,
            JavaPairRDD<Long, Long> clickCategoryId2CountRDD,
            JavaPairRDD<Long, Long> orderCategoryId2CountRDD,
            JavaPairRDD<Long, Long> payCategoryId2CountRDD) {

        JavaPairRDD<Long, Tuple2<Long, org.apache.spark.api.java.Optional<Long>>> tmpJoinRDD =
                categoryidRDD.leftOuterJoin(clickCategoryId2CountRDD);

        JavaPairRDD<Long, String> tmpMapRDD = tmpJoinRDD.mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<Long, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, Optional<Long>>> tuple2) throws Exception {
                        long categoryid = tuple2._1;
                        Optional<Long> optional = tuple2._2._2;
                        long clickCount = 0L;

                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        String value = Constants.FIELD_CATEGORY_ID + "=" + categoryid + "|"
                                + Constants.FIELD_CLICK_COUNT + "=" + clickCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }
                });

        tmpMapRDD = tmpMapRDD.leftOuterJoin(orderCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple2) throws Exception {
                        long categoryid = tuple2._1;
                        String value = tuple2._2._1;

                        Optional<Long> optional = tuple2._2._2;
                        long orderCount = 0L;

                        if (optional.isPresent()) {
                            orderCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }
                });
        tmpMapRDD = tmpMapRDD.leftOuterJoin(payCategoryId2CountRDD).mapToPair(
                new PairFunction<Tuple2<Long, Tuple2<String, Optional<Long>>>, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(Tuple2<Long, Tuple2<String, Optional<Long>>> tuple2) throws Exception {
                        long categoryid = tuple2._1;
                        String value = tuple2._2._1;

                        Optional<Long> optional = tuple2._2._2;
                        long payCount = 0L;

                        if (optional.isPresent()) {
                            payCount = optional.get();
                        }

                        value = value + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount;

                        return new Tuple2<Long, String>(categoryid, value);
                    }
                });

        return tmpMapRDD;
    }


    /**
     * 获取top10活跃session
     * @param spark
     * @param taskId
     * @param top10CategoryList
     * @param sessionid2DetailRDD
     */
    private static void getTop10Session(
            SparkSession spark, long taskId,
            List<Tuple2<CategorySortKey, String>> top10CategoryList, JavaPairRDD<String, Row> sessionid2DetailRDD) {
        /**
         * 第一步：将top10热门品类的id生成RDD
         */
        List<Tuple2<Long, Long>> top10CategoryIdList = new ArrayList<Tuple2<Long, Long>>();

        for (Tuple2<CategorySortKey, String> tuple2 : top10CategoryList) {
            Long categoryid = Long.valueOf(StringUtils.getFieldFromConcatString(
                    tuple2._2, "\\|", Constants.FIELD_CATEGORY_ID));
            top10CategoryIdList.add(new Tuple2<>(categoryid, categoryid));
        }

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        JavaPairRDD<Long, Long> top10CategoryIdRDD = sc.parallelizePairs(top10CategoryIdList);

        /**
         * 第二步：计算top10品类被各session点击的次数
         */
        JavaPairRDD<String, Iterable<Row>> sessionid2DetailsRDD =
                sessionid2DetailRDD.groupByKey();

        JavaPairRDD<Long, String> categoryid2SessionCountRDD =
                sessionid2DetailsRDD.flatMapToPair(
                        new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, Long, String>() {
                            @Override
                            public Iterator<Tuple2<Long, String>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                                String sessionid = tuple2._1;
                                Iterator<Row> iterator = tuple2._2.iterator();

                                Map<Long, Long> categoryCountMap = new HashMap<Long, Long>();

                                while (iterator.hasNext()) {
                                    Row row = iterator.next();

                                    if (row.get(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID) != null) {
                                        long categoryid = row.getLong(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID);

                                        Long count = categoryCountMap.get(categoryid);
                                        if (count == null) {
                                            count = 0L;
                                        }
                                        count++;
                                        categoryCountMap.put(categoryid, count);
                                    }
                                }

                                List<Tuple2<Long, String>> list = new ArrayList<Tuple2<Long, String>>();

                                for (Map.Entry<Long, Long> categoryCountEntry : categoryCountMap.entrySet()) {
                                    long categoryid = categoryCountEntry.getKey();
                                    long count = categoryCountEntry.getValue();
                                    String value = sessionid + "," + count;
                                    list.add(new Tuple2<>(categoryid, value));
                                }

                                return list.iterator();
                            }
                        });

        //获取到top10热门品类被各个session点击的次数
        JavaPairRDD<Long, String> top10CategorySessionCountRDD = top10CategoryIdRDD
                .join(categoryid2SessionCountRDD)
                .mapToPair(
                        new PairFunction<Tuple2<Long, Tuple2<Long, String>>, Long, String>() {
                            @Override
                            public Tuple2<Long, String> call(Tuple2<Long, Tuple2<Long, String>> tuple2) throws Exception {
                                return new Tuple2<Long, String>(tuple2._1, tuple2._2._2);
                            }
                        });
        /**
         * 第三步：分组取TopN算法实现，获取每个品类的top10活跃用户
         */
        JavaPairRDD<Long, Iterable<String>> top10CategorySessionCountsRDD =
                top10CategorySessionCountRDD.groupByKey();

        JavaPairRDD<String, String> top10SessionRDD = top10CategorySessionCountsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<Long, Iterable<String>>, String, String>() {
                    @Override
                    public Iterator<Tuple2<String, String>> call(Tuple2<Long, Iterable<String>> tuple2) throws Exception {
                        long categoryid = tuple2._1;
                        Iterator<String> iterator = tuple2._2.iterator();

                        //定义取top10的排序数组
                        String[] top10Sessions = new String[3];

                        while (iterator.hasNext()) {
                            String sessionCount = iterator.next();
                            long count = Long.valueOf(sessionCount.split(",")[1]);

                            for (int i = 0; i < top10Sessions.length; i++) {
                                if (top10Sessions[i] == null) {
                                    top10Sessions[i] = sessionCount;
                                    break;
                                } else {
                                    long _count = Long.valueOf(top10Sessions[i].split(",")[1]);

                                    if (count > _count) {
                                        for (int j = top10Sessions.length-1; j > i; j--) {
                                            top10Sessions[j] = top10Sessions[j-1];
                                        }
                                        top10Sessions[i] = sessionCount;
                                        break;
                                    }
                                }
                            }
                        }

                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                        ITop10SessionDAO top10SessionDAO = DAOFactory.getTop10SessionDAO();

                        for (String sessionCount : top10Sessions) {
                            if (sessionCount != null) {
                                String sessionid = sessionCount.split(",")[0];
                                long count = Long.valueOf(sessionCount.split(",")[1]);

                                Top10Session top10Session = new Top10Session();
                                top10Session.setTaskid(taskId);
                                top10Session.setCategoryid(categoryid);
                                top10Session.setSessionid(sessionid);
                                top10Session.setClickCount(count);

                                top10SessionDAO.insert(top10Session);

                                list.add(new Tuple2<>(sessionid, sessionid));
                            }
                        }

                        return list.iterator();
                    }
                });

        /**
         * 第四步：获取top10活跃session的明细数据，并写入mysql
         */
        JavaPairRDD<String, Tuple2<String, Row>> sessionDetailRDD =
                top10SessionRDD.join(sessionid2DetailRDD);

//        sessionDetailRDD.foreach(
//                new VoidFunction<Tuple2<String, Tuple2<String, Row>>>() {
//                    @Override
//                    public void call(Tuple2<String, Tuple2<String, Row>> tuple2) throws Exception {
//                        Row row = tuple2._2._2;
//
//                        SessionDetail sessionDetail = new SessionDetail();
//                        sessionDetail.setTaskid(taskId);
//                        try {
//                            sessionDetail.setUserid(row.getLong(Constants.USER_VISIT_ACTION_USER_ID));
//                            sessionDetail.setSessionid(row.getString(Constants.USER_VISIT_ACTION_SESSION_ID));
//                            sessionDetail.setPageid(row.getLong(Constants.USER_VISIT_ACTION_PAGE_ID));
//                            sessionDetail.setActionTime(row.getString(Constants.USER_VISIT_ACTION_ACTION_TIME));
//                            sessionDetail.setSearchKeywords(row.getString(Constants.USER_VISIT_ACTION_SEARCH_KEYWORD));
//                            sessionDetail.setClickCategoryId(row.getLong(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID));
//                            sessionDetail.setClickProductId(row.getLong(Constants.USER_VISIT_ACTION_CLICK_PRODUCT_ID));
//                            sessionDetail.setOrderCategoryIds(row.getString(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS));
//                            sessionDetail.setOrderProductIds(row.getString(Constants.USER_VISIT_ACTION_ORDER_PRODUCT_IDS));
//                            sessionDetail.setPayCategoryIds(row.getString(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS));
//                            sessionDetail.setPayProductIds(row.getString(Constants.USER_VISIT_ACTION_PAY_PRODUCT_IDS));
//                        } catch (Exception e) {
//
//                        }
//
//                        ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
//                        sessionDetailDAO.insert(sessionDetail);
//                    }
//                });

        sessionDetailRDD.foreachPartition(
                new VoidFunction<Iterator<Tuple2<String, Tuple2<String, Row>>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Tuple2<String, Row>>> iterator) throws Exception {
                        List<SessionDetail> sessionDetails = new ArrayList<SessionDetail>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Tuple2<String, Row>> tuple2 = iterator.next();

                            Row row = tuple2._2._2;

                            SessionDetail sessionDetail = new SessionDetail();
                            sessionDetail.setTaskid(taskId);

                            try {
                                sessionDetail.setUserid(row.getLong(Constants.USER_VISIT_ACTION_USER_ID));
                                sessionDetail.setSessionid(row.getString(Constants.USER_VISIT_ACTION_SESSION_ID));
                                sessionDetail.setPageid(row.getLong(Constants.USER_VISIT_ACTION_PAGE_ID));
                                sessionDetail.setActionTime(row.getString(Constants.USER_VISIT_ACTION_ACTION_TIME));
                                sessionDetail.setSearchKeywords(row.getString(Constants.USER_VISIT_ACTION_SEARCH_KEYWORD));
                                sessionDetail.setClickCategoryId(row.getLong(Constants.USER_VISIT_ACTION_CLICK_CATEGORY_ID));
                                sessionDetail.setClickProductId(row.getLong(Constants.USER_VISIT_ACTION_CLICK_PRODUCT_ID));
                                sessionDetail.setOrderCategoryIds(row.getString(Constants.USER_VISIT_ACTION_ORDER_CATEGORY_IDS));
                                sessionDetail.setOrderProductIds(row.getString(Constants.USER_VISIT_ACTION_ORDER_PRODUCT_IDS));
                                sessionDetail.setPayCategoryIds(row.getString(Constants.USER_VISIT_ACTION_PAY_CATEGORY_IDS));
                                sessionDetail.setPayProductIds(row.getString(Constants.USER_VISIT_ACTION_PAY_PRODUCT_IDS));
                            } catch (Exception e) {

                            }

                            sessionDetails.add(sessionDetail);

                            ISessionDetailDAO sessionDetailDAO = DAOFactory.getSessionDetailDAO();
                            sessionDetailDAO.insertBatch(sessionDetails);
                        }


                    }
                });

    }

}
