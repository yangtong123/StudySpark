package com.yt.spark.dao.impl;

import com.yt.spark.dao.IAdStatDAO;
import com.yt.spark.domain.AdStat;
import com.yt.spark.jdbc.JDBCHelper;
import com.yt.spark.model.AdStatQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangtong on 17/5/22.
 */
public class AdStatDAOImpl implements IAdStatDAO {
    @Override
    public void updateBatch(List<AdStat> adStats) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        List<AdStat> insertAdStats = new ArrayList<>();
        List<AdStat> updateAdStats = new ArrayList<>();

        String selectSQL = "SELECT count(*) FROM ad_stat " +
                "WHERE date = ? " +
                "AND province = ? " +
                "AND city = ? " +
                "AND ad_id = ? ";

        for (AdStat adStat : adStats) {
            final AdStatQueryResult queryResult = new AdStatQueryResult();

            Object[] params = new Object[] {
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId()};

            jdbcHelper.executeQuery(selectSQL, params, new JDBCHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    while (rs.next()) {
                        int clickCount = rs.getInt(1);
                        queryResult.setCount(clickCount);
                    }
                }
            });

            int clickCount = queryResult.getCount();

            if (clickCount > 0) {
                updateAdStats.add(adStat);
            } else {
                insertAdStats.add(adStat);
            }
        }

        // 批量插入
        String insertSQL = "INSERT INTO ad_stat VALUES(?,?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<>();

        for (AdStat adStat : insertAdStats) {
            Object[] params = new Object[] {
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId(),
                    adStat.getClickCount()};

            insertParamsList.add(params);
        }

        jdbcHelper.executeBatch(insertSQL, insertParamsList);

        // 批量更新
        String updateSQL = "UPDATE ad_stat SET click_count = ? " +
                "WHERE date = ? " +
                "AND province = ? " +
                "AND city = ? " +
                "AND ad_id = ? ";
        List<Object[]> updateParamsList = new ArrayList<>();

        for (AdStat adStat : updateAdStats) {
            Object[] params = new Object[] {
                    adStat.getClickCount(),
                    adStat.getDate(),
                    adStat.getProvince(),
                    adStat.getCity(),
                    adStat.getAdId()};

            updateParamsList.add(params);
        }

        jdbcHelper.executeBatch(updateSQL, updateParamsList);

    }
}
