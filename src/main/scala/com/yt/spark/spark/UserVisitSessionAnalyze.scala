package com.yt.spark.spark

import com.alibaba.fastjson.JSONObject
import com.yt.spark.conf.{Configuration, Constants}
import com.yt.spark.util.{ParamUtils, StringUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * Created by yangtong on 17/5/9.
  */
class UserVisitSessionAnalyze {

    val spark = getSparkSession()


    /**
      * 获得sparksession
      * @return
      */
    private def getSparkSession(): SparkSession = {
        val local = Configuration.getBoolean(Constants.SPARK_LOCAL)
        if (local) {
            SparkSession.builder().appName("UserVisitSessionAnalyze").master("local[2]").getOrCreate()
        } else {
            SparkSession.builder().appName("UserVisitSessionAnalyze").enableHiveSupport().getOrCreate()
        }
    }

    /**
      * 模拟数据(只有在local模式下才使用)
      * @param spark
      */
    private def mockData(spark: SparkSession): Unit = {
        val local = Configuration.getBoolean(Constants.SPARK_LOCAL)
        if (local) {
            MockData.mockData(spark)
        }
    }

    /**
      * 根据起始日期得到ActionRDD
      * @param spark
      * @param taskParams
      * @return
      */
    private def getActionRDDByDateRange(spark: SparkSession, taskParams: JSONObject): RDD[Row] = {
        val startDate: String = ParamUtils.getParam(taskParams, Constants.PARAM_START_DATE)
        val endDate: String = ParamUtils.getParam(taskParams, Constants.PARAM_END_DATE)

        val sql: String = "select * " +
                "from user_visit_action " +
                "where date >='" + startDate + "' " +
                "and date <='" + endDate + "'"

        val df: DataFrame = spark.sql(sql)

        df.rdd
    }

    private def aggregateBySession(actionRDD: RDD[Row]): Unit = {
        val sessionid2ActionRDD: RDD[(String, Row)] = actionRDD.map{ x => (x.getString(2), x)}

        //对行为数据按session粒度进行分组
        val sessionid2ActionsRDD: RDD[(String, Iterable[Row])] = sessionid2ActionRDD.groupByKey()

        //对每一个session分组进行聚，将session中所有的搜素词和点击品类都聚合起来
        val sessionid2PartAggrInfoRDD = sessionid2ActionsRDD.map{x => {
            val sessionid: String = x._1
            val iterator: Iterator[Row] = x._2.iterator

            val searchKeywordsBuilder: StringBuilder = new StringBuilder("")
            val clickCategoryIdBuilder: StringBuilder = new StringBuilder("")

            //遍历session所有访问行为
            while (iterator.hasNext) {
                //提取每个访问行为的搜索词字段和点击品类字段
                val row: Row = iterator.next()
                val searchKeyword: String = row.getString(5)
                val clickCategoryId: Long = row.getLong(6)

                //首先判断是不是为空，然后判断是不是之前出现过
                if (StringUtils.isNotEmpty(searchKeyword)) {
                    if (!searchKeywordsBuilder.toString.contains(searchKeyword))
                        searchKeywordsBuilder.append(searchKeyword + ",")
                }

                if (clickCategoryId != null) {
                    if (!clickCategoryIdBuilder.toString.contains(String.valueOf(clickCategoryId)))
                        clickCategoryIdBuilder.append(clickCategoryId + ",")
                }
            }

            val searchKeywords = StringUtils.trimComma(searchKeywordsBuilder.toString)
            val clickCategoryIds = StringUtils.trimComma(clickCategoryIdBuilder.toString())
        }}



    }
}

object UserVisitSessionAnalyze {



}
