package com.yt.spark.dao.impl;

import com.yt.spark.dao.ITop10CategoryDAO;
import com.yt.spark.domain.Top10Category;
import com.yt.spark.jdbc.JDBCHelper;

/**
 * Created by yangtong on 17/5/12.
 */
public class Top10CategoryDAOImpl implements ITop10CategoryDAO {
    @Override
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";
        Object[] params = new Object[] {
                top10Category.getTaskid(),
                top10Category.getCategoryid(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getPayCount()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
