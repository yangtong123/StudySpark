package com.yt.spark.dao;

import com.yt.spark.domain.AreaTop3Product;

import java.util.List;

/**
 * Created by yangtong on 17/5/19.
 */
public interface IAreaTop3ProductDAO {
    void insertBatch(List<AreaTop3Product> areaTop3Products);
}
