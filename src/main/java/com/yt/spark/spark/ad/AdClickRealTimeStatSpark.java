package com.yt.spark.spark.ad;

import com.yt.spark.conf.Configuration;
import com.yt.spark.conf.Constants;
import com.yt.spark.util.DateUtils;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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

        // 构建kafka参数map, 地址
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put(Constants.KAFKA_METADATA_BROKER_LIST,
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


        // 计算每5秒中，每天每个用户每个广告的点击量
        JavaPairDStream<String, Long> dailyUserAdClickDStream = adRealTimeLogDStream.mapToPair(
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




        jssc.start();
        jssc.awaitTermination();
        jssc.close();

    }



}
