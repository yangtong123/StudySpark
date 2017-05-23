package com.yt.spark.dao;

import com.yt.spark.domain.AdUserClickCount;

import java.util.List;

/**
 * Created by yangtong on 17/5/22.
 */
public interface IAdUserClickCountDAO {
    /**
     * 批量更新
     * @param adUserClickCounts
     */
    void updateBatch(List<AdUserClickCount> adUserClickCounts);

    /**
     * 根据多个key查询用户广告点击量
     * @param date
     * @param userId
     * @param adId
     * @return
     */
    int findClickCountByMultilKey(String date, long userId, long adId);
}
