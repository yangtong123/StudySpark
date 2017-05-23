package com.yt.spark.dao;

import com.yt.spark.domain.AdStat;

import java.util.List;

/**
 * Created by yangtong on 17/5/22.
 */
public interface IAdStatDAO {
    void updateBatch(List<AdStat> adStats);
}
