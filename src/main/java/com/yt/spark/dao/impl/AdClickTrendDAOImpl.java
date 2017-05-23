package com.yt.spark.dao.impl;

import com.yt.spark.dao.IAdClickTrendDAO;
import com.yt.spark.domain.AdClickTrend;
import com.yt.spark.jdbc.JDBCHelper;
import com.yt.spark.model.AdClickTrendQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangtong on 17/5/23.
 */
public class AdClickTrendDAOImpl implements IAdClickTrendDAO {
    @Override
    public void updateBatch(List<AdClickTrend> adClickTrends) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        List<AdClickTrend> updateAdClickTrends = new ArrayList<>();
        List<AdClickTrend> insertAdClcikTrends = new ArrayList<>();

        String selectSQL = "SELECT count(*) " +
                "FROM ad_click_trend " +
                "WHERE date = ? " +
                "AND hour = ? " +
                "AND minute = ? " +
                "AND ad_id = ? ";

        for (AdClickTrend adClickTrend : adClickTrends) {
            final AdClickTrendQueryResult queryResult = new AdClickTrendQueryResult();

            Object[] params = new Object[] {
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdId()};

            jdbcHelper.executeQuery(selectSQL, params, new JDBCHelper.QueryCallBack() {
                @Override
                public void process(ResultSet rs) throws Exception {
                    if (rs.next()) {
                        int count = rs.getInt(1);
                        queryResult.setCount(count);
                    }
                }
            });

            int count = queryResult.getCount();
            if (count > 0) {
                updateAdClickTrends.add(adClickTrend);
            } else {
                insertAdClcikTrends.add(adClickTrend);
            }
        }

        // 批量更新
        String updateSQL = "UPDATE ad_click_trend SET click_count = ? " +
                "WHERE date = ? " +
                "AND hour = ? " +
                "AND minute = ? " +
                "AND ad_id = ? ";
        List<Object[]> updateParamsList = new ArrayList<>();
        for (AdClickTrend adClickTrend : updateAdClickTrends) {
            Object[] params = new Object[] {
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdId()};

            updateParamsList.add(params);
        }
        jdbcHelper.executeBatch(updateSQL, updateParamsList);

        // 批量插入
        String insertSQL = "INSERT INTO ad_click_trend VALUES(?,?,?,?,?)";

        List<Object[]> insertParamsList = new ArrayList<>();

        for (AdClickTrend adClickTrend : adClickTrends) {
            Object[] params = new Object[] {
                    adClickTrend.getDate(),
                    adClickTrend.getHour(),
                    adClickTrend.getMinute(),
                    adClickTrend.getAdId(),
                    adClickTrend.getClickCount()};

            insertParamsList.add(params);
        }

        jdbcHelper.executeBatch(insertSQL, insertParamsList);
    }
}
