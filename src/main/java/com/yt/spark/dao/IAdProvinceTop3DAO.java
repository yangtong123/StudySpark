package com.yt.spark.dao;

import com.yt.spark.domain.AdProvinceTop3;

import java.util.List;

/**
 * Created by yangtong on 17/5/23.
 */
public interface IAdProvinceTop3DAO {
    void updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}
