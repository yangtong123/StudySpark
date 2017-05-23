package com.yt.spark.dao.impl;

import com.yt.spark.dao.IAdProvinceTop3DAO;
import com.yt.spark.domain.AdProvinceTop3;
import com.yt.spark.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangtong on 17/5/23.
 */
public class AdProvinceTop3DAOImpl implements IAdProvinceTop3DAO {
    @Override
    public void updateBatch(List<AdProvinceTop3> adProvinceTop3s) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        // 首先进行去重
        List<String> dateProvinces = new ArrayList<>();

        for (AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
            String date = adProvinceTop3.getDate();
            String province = adProvinceTop3.getProvince();

            String key = date + "_" + province;

            if (!dateProvinces.contains(key)) {
                dateProvinces.add(key);
            }
        }

        // 批量删除
        String deleteSQL = "DELETE FROM ad_province_top3 WHERE date = ? AND province = ?";
        List<Object[]> deleteParamsList = new ArrayList<>();
        for (String dateProvince : dateProvinces) {
            String[] dateProvinceSplited = dateProvince.split("_");
            Object[] params = new Object[] {
                    dateProvinceSplited[0],
                    dateProvinceSplited[1]};

            deleteParamsList.add(params);
        }
        jdbcHelper.executeBatch(deleteSQL, deleteParamsList);

        // 批量插入
        String insertSQL = "INSERT INTO ad_province_top3 VALUES(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<>();
        for (AdProvinceTop3 adProvinceTop3 : adProvinceTop3s) {
            Object[] params = new Object[] {
                    adProvinceTop3.getDate(),
                    adProvinceTop3.getProvince(),
                    adProvinceTop3.getAdId(),
                    adProvinceTop3.getClickCount()};

            insertParamsList.add(params);
        }
        jdbcHelper.executeBatch(insertSQL, insertParamsList);
    }
}
