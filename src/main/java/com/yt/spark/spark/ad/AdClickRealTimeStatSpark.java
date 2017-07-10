package com.yt.spark.spark.ad;

import com.yt.spark.conf.Configuration;
import com.yt.spark.conf.Constants;
import com.yt.spark.dao.*;
import com.yt.spark.dao.factory.DAOFactory;
import com.yt.spark.domain.*;
import com.yt.spark.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * 广告点击实时统计spark作业
 *
 * Created by yangtong on 17/5/22.
 */
public class AdClickRealTimeStatSpark {
    public static void main(String[] args) throws InterruptedException {
        // 构建spark Streaming上下文
        SparkConf conf = new SparkConf()
                .setMaster("local[2]")
                .setAppName("AdClickRealTimeStatSpark");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        jssc.checkpoint("./checkpoint");

        // 构建kafka参数map, 地址
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list",
                Configuration.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));

        // 构建topic set
        String kafkaTopics = Configuration.getProperty(Constants.KAFKA_TOPICS);
        String[] kafkaTopicsSplited = kafkaTopics.split(",");

        Set<String> topics = new HashSet<>();
        for (String topic : kafkaTopicsSplited) {
            topics.add(topic);
        }

        // 选用kafka direct api, 构建针对kafka集群中指定topic的输入DStream
        // val1, val2， val1没有什么特殊意义，val2中包含了kafka topic中的一条一条的实时日志数据
        JavaPairDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                jssc,
                String.class,
                String.class,
                StringDecoder.class,
                StringDecoder.class,
                kafkaParams,
                topics);

        // timestamp province city userid adid

        // 根据动态黑名单进行过滤
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                filterByBlacklist(adRealTimeLogDStream);

        // 生成动态黑名单
        generateDynamicBlacklist(filteredAdRealTimeLogDStream);

        // 业务一：计算每天各省各城市各广告的点击量（yyyyMMdd_province_city_adid,clickCount）
        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                filteredAdRealTimeLogDStream);

        // 业务二：实时统计每天每个省份的top3
        calculateProvinceTop3Ad(adRealTimeStatDStream);

        // 业务三：实时统计每天每个广告在最近1小时的滑动窗口内的点击行为
        calculateAdClickCountByWindow(adRealTimeLogDStream);

        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }


    private static void testStreamingHA() throws Exception {
        final String checkpointDir = "hdfs://192.168.1.105:9090/streaming_checkpoint";

        /**
         * 如果checkpointDir中的数据存在，那么就会从checkpoint中恢复context，
         * 如果不存在，那么就重新生成一个context
         */
        JavaStreamingContext context = JavaStreamingContext.getOrCreate(checkpointDir,
                new Function0<JavaStreamingContext>() {
                    @Override
                    public JavaStreamingContext call() throws Exception {
                        SparkConf conf = new SparkConf()
                                .setMaster("local[2]")
                                .setAppName("AdClickRealTimeStatSpark");

                        JavaStreamingContext jssc = new JavaStreamingContext(
                                conf, Durations.seconds(5));
                        jssc.checkpoint(checkpointDir);

                        Map<String, String> kafkaParams = new HashMap<String, String>();
                        kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
                                Configuration.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
                        String kafkaTopics = Configuration.getProperty(Constants.KAFKA_TOPICS);
                        String[] kafkaTopicsSplited = kafkaTopics.split(",");
                        Set<String> topics = new HashSet<String>();
                        for(String kafkaTopic : kafkaTopicsSplited) {
                            topics.add(kafkaTopic);
                        }

                        JavaPairInputDStream<String, String> adRealTimeLogDStream = KafkaUtils.createDirectStream(
                                jssc,
                                String.class,
                                String.class,
                                StringDecoder.class,
                                StringDecoder.class,
                                kafkaParams,
                                topics);

                        JavaPairDStream<String, String> filteredAdRealTimeLogDStream =
                                filterByBlacklist(adRealTimeLogDStream);
                        generateDynamicBlacklist(filteredAdRealTimeLogDStream);
                        JavaPairDStream<String, Long> adRealTimeStatDStream = calculateRealTimeStat(
                                filteredAdRealTimeLogDStream);
                        calculateProvinceTop3Ad(adRealTimeStatDStream);
                        calculateAdClickCountByWindow(adRealTimeLogDStream);

                        return jssc;

                    }
                });
        context.start();
        context.awaitTermination();
    }

    /**
     *
     * @param adRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, String> filterByBlacklist(JavaPairDStream<String, String> adRealTimeLogDStream) {
        // 接收到原始的用户点击行为日志后
        // 根据mysql中的动态黑名单，进行实时的黑名单过滤
        // 使用transform算子（将DStream中的每个batch RDD进行处理，转换为任意的其它RDD）
        JavaPairDStream<String, String> filteredAdRealTimeLogDStream = adRealTimeLogDStream.transformToPair(
                new Function<JavaPairRDD<String, String>, JavaPairRDD<String, String>>() {
                    @Override
                    public JavaPairRDD<String, String> call(JavaPairRDD<String, String> rdd) throws Exception {
                        // 从mysql中查询所有黑名单用户
                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                        List<AdBlacklist> adBlacklists = adBlacklistDAO.findAll();

                        List<Tuple2<Long, Boolean>> tuples = new ArrayList<Tuple2<Long, Boolean>>();

                        for (AdBlacklist adBlacklist : adBlacklists) {
                            tuples.add(new Tuple2<>(adBlacklist.getUserId(), true));
                        }

                        JavaSparkContext sc = new JavaSparkContext(rdd.context());
                        JavaPairRDD<Long, Boolean> blacklistRDD = sc.parallelizePairs(tuples);

                        // 将原始的日志rdd映射成<userId, log>
                        JavaPairRDD<Long, Tuple2<String, String>> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, String>, Long, Tuple2<String, String>>() {
                                    @Override
                                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, String> tuple2) throws Exception {
                                        String log = tuple2._2;
                                        String[] logSplited = log.split(" ");
                                        long userId = Long.valueOf(logSplited[3]);

                                        return new Tuple2<Long, Tuple2<String, String>>(userId, tuple2);
                                    }
                                });

                        // 左外连接
                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, org.apache.spark.api.java.Optional<Boolean>>> joinedRDD =
                                mappedRDD.leftOuterJoin(blacklistRDD);

                        JavaPairRDD<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> filteredRDD = joinedRDD.filter(
                                new Function<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, Boolean>() {
                                    @Override
                                    public Boolean call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple2) throws Exception {
                                        Optional<Boolean> optional = tuple2._2._2;

                                        // 如果存在说明是黑名单中的用户
                                        if (optional.isPresent() && optional.get()) {
                                            return false;
                                        }

                                        return true;
                                    }
                                });

                        JavaPairRDD<String, String> resultRDD = filteredRDD.mapToPair(
                                new PairFunction<Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>>, String, String>() {
                                    @Override
                                    public Tuple2<String, String> call(Tuple2<Long, Tuple2<Tuple2<String, String>, Optional<Boolean>>> tuple2) throws Exception {
                                        return tuple2._2._1;
                                    }
                                });


                        return resultRDD;
                    }
                });

        return filteredAdRealTimeLogDStream;
    }


    /**
     * 生成动态黑名单
     * @param filteredAdRealTimeLogDStream
     */
    private static void generateDynamicBlacklist(JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {
        // 计算每5秒中，每天每个用户每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple2) throws Exception {
                        String log = tuple2._2;
                        String[] logSplited = log.split(" ");

                        Date date = new Date(Long.valueOf(logSplited[Constants.KAFKA_LOG_TIMESTAMP]));
                        String dateKey = DateUtils.formatDateKey(date);

                        String userId = logSplited[Constants.KAFKA_LOG_USER_ID];
                        String adId = logSplited[Constants.KAFKA_LOG_AD_ID];

                        // 拼接key
                        String key = dateKey + "_" + userId + "_" + adId;

                        return new Tuple2<String, Long>(key, 1L);
                    }
                });

        // 然后做reduceByKey处理
        JavaPairDStream<String, Long> dailyUserAdClickCountDStream = dailyUserAdClickDStream.reduceByKey(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                });

        // 持久化到mysql
        dailyUserAdClickCountDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdUserClickCount> adUserClickCounts = new ArrayList<AdUserClickCount>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple2 = iterator.next();

                            String[] keySplited = tuple2._1.split("_");
                            String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));

                            long userId = Long.valueOf(keySplited[1]);
                            long adId = Long.valueOf(keySplited[2]);
                            long clickCount = tuple2._2;

                            AdUserClickCount adUserClickCount = new AdUserClickCount();
                            adUserClickCount.setDate(date);
                            adUserClickCount.setUserId(userId);
                            adUserClickCount.setAdId(adId);
                            adUserClickCount.setClickCount(clickCount);

                            adUserClickCounts.add(adUserClickCount);
                        }

                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        adUserClickCountDAO.updateBatch(adUserClickCounts);

                    }
                });
            }
        });



        JavaPairDStream<String, Long> blacklistDStream = dailyUserAdClickCountDStream.filter(
                new Function<Tuple2<String, Long>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Long> tuple2) throws Exception {
                        String key = tuple2._1;
                        String[] keySplited = key.split("_");

                        String date = DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                        long userId = Long.valueOf(keySplited[1]);
                        long adId = Long.valueOf(keySplited[2]);

                        // 从mysql中查询指定日期指定用户对指定广告的点击量
                        IAdUserClickCountDAO adUserClickCountDAO = DAOFactory.getAdUserClickCountDAO();
                        int clickCount = adUserClickCountDAO.findClickCountByMultilKey(date, userId, adId);

                        // 如果大于100，那么就是黑名单用户
                        if (clickCount >= 100) {
                            return true;
                        }

                        return false;
                    }
                });

        // 可能存在重复的userId
        JavaDStream<Long> blacklistUseridDStream = blacklistDStream.map(
                new Function<Tuple2<String, Long>, Long>() {
                    @Override
                    public Long call(Tuple2<String, Long> tuple2) throws Exception {
                        String key = tuple2._1;
                        String[] keySplited = key.split("_");
                        Long userId = Long.valueOf(keySplited[1]);
                        return userId;
                    }
                });

        JavaDStream<Long> distinctBlacklistUseridDStream = blacklistUseridDStream.transform(
                new Function<JavaRDD<Long>, JavaRDD<Long>>() {
                    @Override
                    public JavaRDD<Long> call(JavaRDD<Long> rdd) throws Exception {
                        return rdd.distinct();
                    }
                });

        distinctBlacklistUseridDStream.foreachRDD(new VoidFunction<JavaRDD<Long>>() {
            @Override
            public void call(JavaRDD<Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Long>>() {
                    @Override
                    public void call(Iterator<Long> iterator) throws Exception {
                        List<AdBlacklist> adBlacklists = new ArrayList<AdBlacklist>();

                        while (iterator.hasNext()) {
                            long userId = iterator.next();
                            AdBlacklist adBlacklist = new AdBlacklist();
                            adBlacklist.setUserId(userId);

                            adBlacklists.add(adBlacklist);
                        }

                        IAdBlacklistDAO adBlacklistDAO = DAOFactory.getAdBlacklistDAO();
                        adBlacklistDAO.insertBatch(adBlacklists);
                    }
                });
            }
        });
    }


    /**
     * 广告点击流量实时统计
     * @param filteredAdRealTimeLogDStream
     * @return
     */
    private static JavaPairDStream<String, Long> calculateRealTimeStat(
            JavaPairDStream<String, String> filteredAdRealTimeLogDStream) {

        JavaPairDStream<String, Long> mappedDStream = filteredAdRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple2) throws Exception {
                        String log = tuple2._2;
                        String[] logSplited = log.split(" ");

                        Date date = new Date(Long.valueOf(logSplited[0]));
                        String dateKey = DateUtils.formatDateKey(date);

                        String province = logSplited[1];
                        String city = logSplited[2];
                        String adId = logSplited[4];

                        String key = dateKey + "_" + province + "_" + city + "_" + adId;

                        return new Tuple2<String, Long>(key, 1L);
                    }
                });

        // 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
        JavaPairDStream<String, Long> aggreateDStream = mappedDStream.updateStateByKey(
                new Function2<List<Long>, Optional<Long>, Optional<Long>>() {
                    @Override
                    public Optional<Long> call(List<Long> values, Optional<Long> optional) throws Exception {
                        long clickCount = 0L;

                        if (optional.isPresent()) {
                            clickCount = optional.get();
                        }

                        for (Long value : values) {
                            clickCount += value;
                        }

                        return optional.of(clickCount);
                    }
                });

        // 将计算出来的最新结果，同步一份到mysql中，以便于j2ee系统使用
        aggreateDStream.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdStat> adStats = new ArrayList<AdStat>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple2 = iterator.next();

                            String[] keySplited = tuple2._1.split("_");
                            String date = keySplited[0];
                            String province = keySplited[1];
                            String city = keySplited[2];
                            long adId = Long.valueOf(keySplited[3]);

                            long clickCount = tuple2._2;

                            AdStat adStat = new AdStat();
                            adStat.setDate(date);
                            adStat.setProvince(province);
                            adStat.setCity(city);
                            adStat.setAdId(adId);
                            adStat.setClickCount(clickCount);

                            adStats.add(adStat);
                        }

                        IAdStatDAO adStatDAO = DAOFactory.getAdStatDAO();
                        adStatDAO.updateBatch(adStats);

                    }
                });
            }
        });

        return aggreateDStream;
    }


    /**
     * 计算每天各省份的top3热门广告
     * @param adRealTimeStatDStream
     */
    private static void calculateProvinceTop3Ad(
            JavaPairDStream<String, Long> adRealTimeStatDStream) {

        // 得到各个省份最热门的top3广告
        JavaDStream<Row> rowsDStream = adRealTimeStatDStream.transform(
                new Function<JavaPairRDD<String, Long>, JavaRDD<Row>>() {
                    @Override
                    public JavaRDD<Row> call(JavaPairRDD<String, Long> rdd) throws Exception {

                        // 计算出每天各省份各广告的点击量
                        JavaPairRDD<String, Long> mappedRDD = rdd.mapToPair(
                                new PairFunction<Tuple2<String, Long>, String, Long>() {
                                    @Override
                                    public Tuple2<String, Long> call(Tuple2<String, Long> tuple2) throws Exception {
                                        String[] keySplited = tuple2._1.split("_");
                                        String date = keySplited[0];
                                        String province = keySplited[1];
                                        long adId = Long.valueOf(keySplited[3]);
                                        long clickCount = tuple2._2;

                                        String key = date + "_" + province + "_" + adId;

                                        return new Tuple2<String, Long>(key, clickCount);
                                    }
                                });

                        JavaPairRDD<String, Long> dailyAdClickCountByProvinceRDD = mappedRDD.reduceByKey(
                                new Function2<Long, Long, Long>() {
                                    @Override
                                    public Long call(Long v1, Long v2) throws Exception {
                                        return v1 + v2;
                                    }
                                });

                        //  将dailyAdClickCountByProvinceRDD转换为DataFram，并注册成一张临时表
                        JavaRDD<Row> rowsRDD = dailyAdClickCountByProvinceRDD.map(
                                new Function<Tuple2<String, Long>, Row>() {
                                    @Override
                                    public Row call(Tuple2<String, Long> tuple2) throws Exception {
                                        String[] keySplited = tuple2._1.split("_");
                                        String dateKey = keySplited[0];
                                        String date = DateUtils.formatDate(DateUtils.parseDateKey(dateKey));

                                        String province = keySplited[1];
                                        long adId = Long.valueOf(keySplited[2]);
                                        long clickCount = tuple2._2;


                                        return RowFactory.create(date, province, adId, clickCount);
                                    }
                                });

                        StructType schema = DataTypes.createStructType(Arrays.asList(
                                DataTypes.createStructField("date", DataTypes.StringType, true),
                                DataTypes.createStructField("province", DataTypes.StringType, true),
                                DataTypes.createStructField("ad_id", DataTypes.LongType, true),
                                DataTypes.createStructField("click_count", DataTypes.LongType, true)));

                        SparkSession spark = new SparkSession(rdd.context());

                        Dataset dailyAdClickCountByProvinceDF = spark.createDataFrame(rowsRDD, schema);

                        dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");

                        // 使用spark SQL 配合开窗函数，统计出各省份top3热门的广告
                        String sql =
                                "SELECT " +
                                    "date, " +
                                    "province, " +
                                    "ad_id, " +
                                    "click_count " +
                                "FROM (" +
                                    "SELECT " +
                                        "date, " +
                                        "province, " +
                                        "ad_id, " +
                                        "click_count, " +
                                        "ROW_NUMBER() OVER (PARTITION BY province ORDER BY click_count DESC) rank " +
                                    "FROM tmp_daily_ad_click_count_by_prov " +
                                ") t " +
                                "WHERE rank >= 3";


                        Dataset<Row> provinceTop3AdDF = spark.sql(sql);

                        return provinceTop3AdDF.javaRDD();
                    }
                });

        // 将rowsDStream批量更新到mysql中
        rowsDStream.foreachRDD(new VoidFunction<JavaRDD<Row>>() {
            @Override
            public void call(JavaRDD<Row> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> iterator) throws Exception {
                        List<AdProvinceTop3> adProvinceTop3s = new ArrayList<AdProvinceTop3>();

                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            String date = row.getString(0);
                            String province = row.getString(1);
                            long adId = row.getLong(2);
                            long clickCount = row.getLong(3);

                            AdProvinceTop3 adProvinceTop3 = new AdProvinceTop3();
                            adProvinceTop3.setDate(date);
                            adProvinceTop3.setProvince(province);
                            adProvinceTop3.setAdId(adId);
                            adProvinceTop3.setClickCount(clickCount);

                            adProvinceTop3s.add(adProvinceTop3);
                        }

                        IAdProvinceTop3DAO adProvinceTop3DAO = DAOFactory.getAdProvinceTop3DAO();
                        adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                    }
                });
            }
        });

    }


    /**
     * 计算最近1小时滑动窗口内的广告点击趋势
     * @param adRealTimeLogDStream
     */
    private static void calculateAdClickCountByWindow(
            JavaPairDStream<String, String> adRealTimeLogDStream) {

        // <yyyyMMddHHmm_adId, 1L>
        JavaPairDStream<String, Long> pairDStream = adRealTimeLogDStream.mapToPair(
                new PairFunction<Tuple2<String, String>, String, Long>() {
                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple2) throws Exception {
                        // timestamp province city userId adId
                        String[] logSplited = tuple2._2().split(" ");
                        String timeMinute = DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
                        long adId = Long.valueOf(logSplited[4]);

                        return new Tuple2<String, Long>(timeMinute + "_" + adId, 1L);
                    }
                });


        //
        JavaPairDStream<String, Long> aggrRDD = pairDStream.reduceByKeyAndWindow(
                new Function2<Long, Long, Long>() {
                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }
                }, Durations.minutes(60), Durations.seconds(10));


        aggrRDD.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            @Override
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, Long>> iterator) throws Exception {
                        List<AdClickTrend> adClickTrends = new ArrayList<AdClickTrend>();

                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple2 = iterator.next();
                            String[] keySplited = tuple2._1.split("_");
                            //yyyyMMddHHmm
                            String dateMinute = keySplited[0];
                            long adId = Long.valueOf(keySplited[1]);
                            long clickCount = tuple2._2;

                            String date = DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0, 8)));
                            String hour = dateMinute.substring(8, 10);
                            String minute = dateMinute.substring(10);

                            AdClickTrend adClickTrend = new AdClickTrend();
                            adClickTrend.setDate(date);
                            adClickTrend.setHour(hour);
                            adClickTrend.setMinute(minute);
                            adClickTrend.setAdId(adId);
                            adClickTrend.setClickCount(clickCount);

                            adClickTrends.add(adClickTrend);
                        }

                        IAdClickTrendDAO adClickTrendDAO = DAOFactory.getAdClickTrendDAO();
                        adClickTrendDAO.updateBatch(adClickTrends);
                    }
                });
            }
        });
    }

}
