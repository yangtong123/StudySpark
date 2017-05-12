package com.yt.spark.dao.impl;

import com.yt.spark.dao.ITop10SessionDAO;
import com.yt.spark.domain.Top10Session;
import com.yt.spark.jdbc.JDBCHelper;

/**
 * Created by yangtong on 17/5/12.
 */
public class Top10SessionDAOImpl implements ITop10SessionDAO {
    @Override
    public void insert(Top10Session top10Session) {
        String sql = "insert into top10_category_session values(?,?,?,?)";
        Object[] params = new Object[] {
                top10Session.getTaskid(),
                top10Session.getCategoryid(),
                top10Session.getSessionid(),
                top10Session.getClickCount()
        };

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
