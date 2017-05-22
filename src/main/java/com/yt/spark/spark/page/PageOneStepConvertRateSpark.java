package com.yt.spark.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.yt.spark.conf.Constants;
import com.yt.spark.dao.IPageSplitConvertRateDAO;
import com.yt.spark.dao.ITaskDAO;
import com.yt.spark.dao.factory.DAOFactory;
import com.yt.spark.domain.PageSplitConvertRate;
import com.yt.spark.domain.Task;
import com.yt.spark.util.DateUtils;
import com.yt.spark.util.NumberUtils;
import com.yt.spark.util.ParamUtils;
import com.yt.spark.util.SparkUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

/**
 * 页面切片转化率spark程序
 * Created by yangtong on 17/5/18.
 */
public class PageOneStepConvertRateSpark {
    public static void main(String[] args) {
        //1.构建spark上下文
        SparkSession spark = SparkUtils.getSparkSession(Constants.SPARK_APP_NAME_PAGE);

        //2.生成模拟数据
        SparkUtils.mockData(spark);

        //3.查询任务，获取任务参数
        long taskid = ParamUtils.getTaskIdFromArgs(args, Constants.SPARK_LOCAL_TASKID_PAGE);

        ITaskDAO taskDAO = DAOFactory.getTaskDAO();
        Task task = taskDAO.findById(taskid);

        if (task == null) {
            System.out.println(new Date() + ": cannot find this task with id : " + taskid + " .");
            return;
        }

        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //4.查询指定日期范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtils.getActionRDDByDateRange(spark, taskParam);

        JavaPairRDD<String, Row> sessionid2ActionRDD = getSessionid2ActionRDD(actionRDD);
        sessionid2ActionRDD = sessionid2ActionRDD.cache();

        JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD = sessionid2ActionRDD.groupByKey();

        //核心！每个session单跳页面生成
        JavaPairRDD<String, Integer> pageSplitRDD = generateAndMatchPageSplit(spark, sessionid2ActionsRDD, taskParam);

        Map<String, Long> pageSplitPvMap = pageSplitRDD.countByKey();


        Long startPagePv = getStartPagePv(taskParam, sessionid2ActionsRDD);


        //计算目标页面流的各个页面切片的转化率
        Map<String, Double> convertRateMap = computePageSplitConvertRate(taskParam, pageSplitPvMap, startPagePv);


        //持久化
        persistConvertRate(taskid, convertRateMap);


        spark.close();
    }


    /**
     * 得到sessionid2ActionRDD
     * @param actionRDD
     * @return
     */
    private static JavaPairRDD<String, Row> getSessionid2ActionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(
                new PairFunction<Row, String, Row>() {
                    @Override
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        return new Tuple2<String, Row>(row.getString(Constants.USER_VISIT_ACTION_SESSION_ID), row);
                    }
                });
    }

    /**
     *
     * @param spark
     * @param sessionid2ActionsRDD
     * @param taskParam
     * @return
     */
    private static JavaPairRDD<String, Integer> generateAndMatchPageSplit(SparkSession spark,
                                                                          JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD,
                                                                          JSONObject taskParam) {

        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        //将这个targetPageFlow做成broadcast
        final Broadcast<String> targetPageFlowBroadcast = new JavaSparkContext(spark.sparkContext()).broadcast(targetPageFlow);

        return sessionid2ActionsRDD.flatMapToPair(
                new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Integer>() {
                    @Override
                    public Iterator<Tuple2<String, Integer>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                        //返回的list
                        List<Tuple2<String, Integer>> list = new ArrayList<Tuple2<String, Integer>>();

                        Iterator<Row> iterator = tuple2._2.iterator();

                        //获取使用者指定的页面流
                        String[] targetPages = targetPageFlowBroadcast.value().split(",");

                        //对session的访问行为进行排序
                        List<Row> rows = new LinkedList<Row>();
                        while (iterator.hasNext()) {
                            rows.add(iterator.next());
                        }
                        Collections.sort(rows, new Comparator<Row>() {
                            @Override
                            public int compare(Row o1, Row o2) {
                                String actionTime1 = o1.getString(Constants.USER_VISIT_ACTION_ACTION_TIME);
                                String actionTime2 = o2.getString(Constants.USER_VISIT_ACTION_ACTION_TIME);

                                Date date1 = DateUtils.parseTime(actionTime1);
                                Date date2 = DateUtils.parseTime(actionTime2);

                                return (int) (date1.getTime() - date2.getTime());
                            }
                        });

                        //页面切片的生成，以及页面流的匹配
                        Long lastPageId = null;

                        for (Row row : rows) {
                            long pageId = row.getLong(Constants.USER_VISIT_ACTION_PAGE_ID);

                            if (lastPageId == null) {
                                lastPageId = pageId;
                                continue;
                            }

                            //生成一个页面切片
                            String pageSplit = lastPageId + "_" + pageId;

                            //判断这个切片是否在用户指定的页面流中
                            for (int i = 1; i < targetPages.length; i++) {
                                String targetPageSplit = targetPages[i-1] + "_" + targetPages[i];
                                if (pageSplit.equals(targetPageSplit)) {
                                    list.add(new Tuple2<>(pageSplit, 1));
                                    break;
                                }
                            }

                            lastPageId = pageId;
                        }
                        return list.iterator();
                    }
                });
    }


    /**
     * 获取取页面流中初始页面的pv
     * @param taskParam
     * @param sessionid2ActionsRDD
     * @return
     */
    private static Long getStartPagePv(JSONObject taskParam,
                                       JavaPairRDD<String, Iterable<Row>> sessionid2ActionsRDD) {

        String targetPageFlow = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW);
        final Long startPageId = Long.valueOf(targetPageFlow.split(",")[0]);

        JavaRDD<Long> startPageRDD = sessionid2ActionsRDD.flatMap(
                new FlatMapFunction<Tuple2<String, Iterable<Row>>, Long>() {
                    @Override
                    public Iterator<Long> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {
                        List<Long> list = new ArrayList<Long>();

                        Iterator<Row> iterator = tuple2._2.iterator();

                        while (iterator.hasNext()) {
                            Row row = iterator.next();
                            long pageId = row.getLong(Constants.USER_VISIT_ACTION_PAGE_ID);

                            if (pageId == startPageId) {
                                list.add(pageId);
                            }
                        }

                        return list.iterator();
                    }
                });

        return startPageRDD.count();
    }


    /**
     * 计算页面切片转化率
     * @param taskParam
     * @param pageSplitPvMap
     * @param startPagePv
     * @return
     */
    private static Map<String, Double> computePageSplitConvertRate(JSONObject taskParam,
                                                                   Map<String, Long> pageSplitPvMap,
                                                                   long startPagePv) {
        Map<String, Double> convertRateMap = new HashMap<>();

        String[] targetPages = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        long lastPageSplitPv = 0L;

        //3,5,2,4,6
        //3_5
        //3_5 pv /3 pv
        //5_2 rate = 5_2 pv / 3_5 pv
        for (int i = 1; i < targetPages.length; i++) {
            String targetpageSplit = targetPages[i-1] + "_" + targetPages[i];
            long targetPageSplitPv = Long.valueOf(String.valueOf(pageSplitPvMap.get(targetpageSplit)));

            double convertRate = 0.0;

            if (i == 1) {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) startPagePv, 2);
            } else {
                convertRate = NumberUtils.formatDouble((double) targetPageSplitPv / (double) lastPageSplitPv, 2);
            }

            convertRateMap.put(targetpageSplit, convertRate);

            lastPageSplitPv = targetPageSplitPv;
        }

        return  convertRateMap;
    }


    private static void persistConvertRate(long taskid, Map<String, Double> convertRateMap) {
        StringBuffer buffer = new StringBuffer("");

        for (Map.Entry<String, Double> convertRateEntry : convertRateMap.entrySet()) {
            String pageSplit = convertRateEntry.getKey();
            double convertRate = convertRateEntry.getValue();
            buffer.append(pageSplit + "=" + convertRate + "|");
        }

        String convertRate = buffer.toString();
        convertRate = convertRate.substring(0, convertRate.length() - 1);

        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskid(taskid);
        pageSplitConvertRate.setConvertRate(convertRate);

        IPageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);
    }

}
