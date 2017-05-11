package com.yt.spark.dao.impl;

import com.yt.spark.dao.ISessionRandomExtractDAO;
import com.yt.spark.domain.SessionRandomExtract;
import com.yt.spark.jdbc.JDBCHelper;

/**
 * Created by yangtong on 17/5/11.
 */
public class SessionRandomExtractDAOImpl implements ISessionRandomExtractDAO {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        Object[] params = new Object[] {
                sessionRandomExtract.getTaskid(),
                sessionRandomExtract.getSessionid(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSearchKeywords(),
                sessionRandomExtract.getClickCategoryIds()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
