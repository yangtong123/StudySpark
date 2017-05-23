package com.yt.spark.dao.impl;

import com.yt.spark.dao.IAdUserClickCountDAO;
import com.yt.spark.domain.AdUserClickCount;
import com.yt.spark.jdbc.JDBCHelper;
import com.yt.spark.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangtong on 17/5/22.
 */
public class AdUserClickCountDAOImpl implements IAdUserClickCountDAO {
    @Override
    public void updateBatch(List<AdUserClickCount> adUserClickCounts) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        List<AdUserClickCount> insertAdUserClickCounts = new ArrayList<>();
        List<AdUserClickCount> updateAdUserClickCounts = new ArrayList<>();

        String selectSQL = "SELECT count(*) FROM ad_user_click_count " +
                "WHERE date = ? AND user_id = ? AND ad_id=?";
        Object[] selectParams = null;

        for (AdUserClickCount adUserClickCount : adUserClickCounts) {
            final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

            selectParams = new Object[] {adUserClickCount.getDate(),
                    adUserClickCount.getUserId(),
                    adUserClickCount.getAdId()};

            jdbcHelper.executeQuery(selectSQL, selectParams, new JDBCHelper.QueryCallBack() {
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
                updateAdUserClickCounts.add(adUserClickCount);
            } else {
                insertAdUserClickCounts.add(adUserClickCount);
            }
        }

        // 执行批量插入
        String insertSQL = "INSERT INTO ad_user_click_count VALUES(?,?,?,?)";
        List<Object[]> insertParamsList = new ArrayList<Object[]>();

        for (AdUserClickCount adUserClickCount : insertAdUserClickCounts) {
            Object[] insertParams = new Object[] {adUserClickCount.getDate(),
                    adUserClickCount.getUserId(),
                    adUserClickCount.getAdId(),
                    adUserClickCount.getClickCount()};

            insertParamsList.add(insertParams);
        }

        jdbcHelper.executeBatch(insertSQL, insertParamsList);


        // 执行批量更新, 应该是累加操作
        String updateSQL = "UPDATE ad_user_click_count SET click_count = click_count + ? " +
                "WHERE date = ? AND user_id = ? AND ad_id = ?";
        List<Object[]> updateParamsList = new ArrayList<>();

        for (AdUserClickCount adUserClickCount : updateAdUserClickCounts) {
            Object[] updateParams = new Object[] {adUserClickCount.getDate(),
                    adUserClickCount.getUserId(),
                    adUserClickCount.getAdId()};

            updateParamsList.add(updateParams);
        }

        jdbcHelper.executeBatch(updateSQL, updateParamsList);
    }

    /**
     *
     * @param date
     * @param userId
     * @param adId
     * @return
     */
    @Override
    public int findClickCountByMultilKey(String date, long userId, long adId) {
        String sql =
                "SELECT click_count " +
                "FROM ad_user_click_count " +
                "WHERE date = ? " +
                "AND user_id = ? " +
                "AND ad_id = ?";

        Object[] params = new Object[]{date, userId, adId};

        final AdUserClickCountQueryResult queryResult = new AdUserClickCountQueryResult();

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql, params, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                if (rs.next()) {
                    int clickCount = rs.getInt(1);
                    queryResult.setCount(clickCount);
                }
            }
        });

        int clickCount = queryResult.getCount();

        return clickCount;
    }
}
