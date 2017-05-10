package com.yt.spark.dao;

import com.yt.spark.domain.SessionAggrStat;

/**
 * Created by yangtong on 17/5/10.
 */
public interface ISessionAggrStatDAO {
    /**
     * session聚合统计DAO
     * @param sessionAggrStat
     */
    void insert(SessionAggrStat sessionAggrStat);
}
