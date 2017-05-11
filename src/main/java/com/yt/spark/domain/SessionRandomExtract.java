package com.yt.spark.domain;

/**
 * Session随机抽取
 * Created by yangtong on 17/5/11.
 */
public class SessionRandomExtract {
    private long taskid;
    private String sessionid;
    private String startTime;
    private String searchKeywords;
    private String clickCategoryIds;

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getSessionid() {
        return sessionid;
    }

    public void setSessionid(String sessionid) {
        this.sessionid = sessionid;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getSearchKeywords() {
        return searchKeywords;
    }

    public void setSearchKeywords(String searchKeywords) {
        this.searchKeywords = searchKeywords;
    }

    public String getClickCategoryIds() {
        return clickCategoryIds;
    }

    public void setClickCategoryIds(String clickCategoryIds) {
        this.clickCategoryIds = clickCategoryIds;
    }
}
