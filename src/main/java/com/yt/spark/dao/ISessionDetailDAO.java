package com.yt.spark.dao;

import com.yt.spark.domain.SessionDetail;

import java.util.List;

/**
 * SessionDetailDAO
 * Created by yangtong on 17/5/11.
 */
public interface ISessionDetailDAO {
    /**
     * insert一条明细数据
     * @param sessionDetail
     */
    void insert(SessionDetail sessionDetail);

    /**
     * 批量插入session明细数据
     * @param sessionDetails
     */
    void insertBatch(List<SessionDetail> sessionDetails);
}
