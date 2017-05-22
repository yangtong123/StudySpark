package com.yt.spark.conf;

/**
 * Created by yangtong on 17/5/5.
 */
public interface Constants {
    /**
     * 项目配置相关的常量
     */
    String JDBC_DRIVER = "jdbc.driver";
    String JDBC_DATASOURCE_SIZE = "jdbc.datasource.size";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.user";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_URL_PROD = "jdbc.url.prod";
    String JDBC_USER_PROD = "jdbc.user.prod";
    String JDBC_PASSWORD_PROD = "jdbc.password.prod";
    String SPARK_LOCAL = "spark.local";
    String SPARK_LOCAL_TASKID_SESSION = "spark.local.taskid.session";
    String SPARK_LOCAL_TASKID_PAGE = "spark.local.taskid.page";
    String SPARK_LOCAL_TASKID_PRODUCT = "spark.local.taskid.product";
    String KAFKA_METADATA_BROKER_LIST = "kafka.metadata.broker.list";
    String KAFKA_TOPICS = "kafka.topics";

    /**
     * Spark作业相关的常量
     */
    String SPARK_APP_NAME_SESSION = "UserVisitSessionAnalyzeSpark";
    String SPARK_APP_NAME_PAGE = "PageOneStepConvertRateSpark";
    String SPARK_APP_NAME_PRODUCT = "AreaTop3ProductSpark";
    String FIELD_SESSION_ID = "sessionid";
    String FIELD_SEARCH_KEYWORDS = "searchKeywords";
    String FIELD_CLICK_CATEGORY_IDS = "clickCategoryIds";
    String FIELD_AGE = "age";
    String FIELD_PROFESSIONAL = "professional";
    String FIELD_CITY = "city";
    String FIELD_SEX = "sex";
    String FIELD_VISIT_LENGTH = "visitLength";
    String FIELD_STEP_LENGTH = "stepLength";
    String FIELD_START_TIME = "startTime";
    String FIELD_CLICK_COUNT = "clickCount";
    String FIELD_ORDER_COUNT = "orderCount";
    String FIELD_PAY_COUNT = "payCount";
    String FIELD_CATEGORY_ID = "categoryid";

    String SESSION_COUNT = "session_count";

    String TIME_PERIOD_1s_3s = "1s_3s";
    String TIME_PERIOD_4s_6s = "4s_6s";
    String TIME_PERIOD_7s_9s = "7s_9s";
    String TIME_PERIOD_10s_30s = "10s_30s";
    String TIME_PERIOD_30s_60s = "30s_60s";
    String TIME_PERIOD_1m_3m = "1m_3m";
    String TIME_PERIOD_3m_10m = "3m_10m";
    String TIME_PERIOD_10m_30m = "10m_30m";
    String TIME_PERIOD_30m = "30m";

    String STEP_PERIOD_1_3 = "1_3";
    String STEP_PERIOD_4_6 = "4_6";
    String STEP_PERIOD_7_9 = "7_9";
    String STEP_PERIOD_10_30 = "10_30";
    String STEP_PERIOD_30_60 = "30_60";
    String STEP_PERIOD_60 = "60";

    /**
     * 任务相关的常量
     */
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";
    String PARAM_START_AGE = "startAge";
    String PARAM_END_AGE = "endAge";
    String PARAM_PROFESSIONALS = "professionals";
    String PARAM_CITIES = "cities";
    String PARAM_SEX = "sex";
    String PARAM_KEYWORDS = "keywords";
    String PARAM_CATEGORY_IDS = "categoryIds";
    String PARAM_TARGET_PAGE_FLOW = "targetPageFlow";


    /**
     * Dateset相关常量
     */
    int USER_VISIT_ACTION_DATE = 0;
    int USER_VISIT_ACTION_USER_ID = 1;
    int USER_VISIT_ACTION_SESSION_ID = 2;
    int USER_VISIT_ACTION_PAGE_ID = 3;
    int USER_VISIT_ACTION_ACTION_TIME = 4;
    int USER_VISIT_ACTION_SEARCH_KEYWORD = 5;
    int USER_VISIT_ACTION_CLICK_CATEGORY_ID = 6;
    int USER_VISIT_ACTION_CLICK_PRODUCT_ID = 7;
    int USER_VISIT_ACTION_ORDER_CATEGORY_IDS = 8;
    int USER_VISIT_ACTION_ORDER_PRODUCT_IDS = 9;
    int USER_VISIT_ACTION_PAY_CATEGORY_IDS = 10;
    int USER_VISIT_ACTION_PAY_PRODUCT_IDS = 11;
    int USER_VISIT_ACTION_CITY_ID = 12;

    int USER_INFO_USER_ID = 0;
    int USER_INFO_USERNAME = 1;
    int USER_INFO_NAME = 2;
    int USER_INFO_AGE = 3;
    int USER_INFO_PROFESSIONAL = 4;
    int USER_INFO_CITY = 5;
    int USER_INFO_SEX = 6;

    int PRODUCT_INFO_PRODUCT_ID = 0;
    int PRODUCT_INFO_PRODUCT_NAME = 1;
    int PRODUCT_INFO_EXTEND_INFO = 2;

    /**
     * kafka_log相关常量
     */
    int KAFKA_LOG_TIMESTAMP = 0;
    int KAFKA_LOG_PROVINCE = 1;
    int KAFKA_LOG_CITY = 2;
    int KAFKA_LOG_USER_ID = 3;
    int KAFKA_LOG_AD_ID = 4;
}
