package com.yt.spark.dao.impl;

import com.yt.spark.dao.IPageSplitConvertRateDAO;
import com.yt.spark.domain.PageSplitConvertRate;
import com.yt.spark.jdbc.JDBCHelper;

/**
 * Created by yangtong on 17/5/18.
 */
public class PageSplitConvertRateDAOImpl implements IPageSplitConvertRateDAO {
    @Override
    public void insert(PageSplitConvertRate pageSplitConvertRate) {
        String sql = "insert into page_split_convert_rate values(?,?)";

        Object[] params = new Object[] {
                pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, params);
    }
}
