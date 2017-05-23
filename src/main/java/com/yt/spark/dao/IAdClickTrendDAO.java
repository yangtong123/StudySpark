package com.yt.spark.dao;

import com.yt.spark.domain.AdClickTrend;

import java.util.List;

/**
 * Created by yangtong on 17/5/23.
 */
public interface IAdClickTrendDAO {

    void updateBatch(List<AdClickTrend> adClickTrends);
}
