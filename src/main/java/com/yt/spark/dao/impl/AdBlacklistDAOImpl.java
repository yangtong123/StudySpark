package com.yt.spark.dao.impl;

import com.yt.spark.dao.IAdBlacklistDAO;
import com.yt.spark.domain.AdBlacklist;
import com.yt.spark.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by yangtong on 17/5/22.
 */
public class AdBlacklistDAOImpl implements IAdBlacklistDAO {
    @Override
    public void insertBatch(List<AdBlacklist> adBlacklists) {
        String sql = "INSERT INTO ad_blacklist VALUES(?)";

        List<Object[]> paramsList = new ArrayList<>();

        for (AdBlacklist adBlacklist : adBlacklists) {
            Object[] params = new Object[] {adBlacklist.getUserId()};
            paramsList.add(params);
        }

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeBatch(sql, paramsList);
    }

    @Override
    public List<AdBlacklist> findAll() {
        String sql = "SELECT * FROM ad_blacklist";

        final List<AdBlacklist> adBlacklists = new ArrayList<>();

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeQuery(sql, null, new JDBCHelper.QueryCallBack() {
            @Override
            public void process(ResultSet rs) throws Exception {
                while (rs.next()) {
                    long userId = Long.valueOf(String.valueOf(rs.getInt(1)));

                    AdBlacklist adBlacklist = new AdBlacklist();
                    adBlacklist.setUserId(userId);

                    adBlacklists.add(adBlacklist);
                }
            }
        });

        return adBlacklists;
    }
}
