package com.yt.spark.domain;

/**
 * Created by yangtong on 17/5/18.
 */
public class PageSplitConvertRate {
    private long taskid;
    private String convertRate;

    public long getTaskid() {
        return taskid;
    }

    public void setTaskid(long taskid) {
        this.taskid = taskid;
    }

    public String getConvertRate() {
        return convertRate;
    }

    public void setConvertRate(String convertRate) {
        this.convertRate = convertRate;
    }
}
