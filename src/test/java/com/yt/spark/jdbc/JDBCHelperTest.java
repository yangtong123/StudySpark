package com.yt.spark.jdbc;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by yangtong on 17/5/5.
 */
public class JDBCHelperTest {
    public static void main(String[] args) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        //测试更新语句
        //jdbcHelper.executeUpdate("insert into test_jdbc(name) values(?)", new Object[] {"asdf"});

        //测试查询语句
        Map<String, Object>  testUser = new HashMap<String, Object>();

        jdbcHelper.executeQuery(
                "select id, name from test_jdbc where id = ?",
                new Object[]{2},
                rs -> {  //jdk8 中的lambda表达式
                    if (rs.next()) {
                        int id = rs.getInt(1);
                        String name = rs.getString(2);

                        testUser.put("id", id);
                        testUser.put("name", name);
                    }
                });
        System.out.println(testUser.get("id") + " : " + testUser.get("name"));

        //测试批量执行sql语句
        String sql = "insert into test_jdbc(name) values(?)";
        List<Object []> paramsList = new ArrayList<Object[]>();
        paramsList.add(new Object[] {"asdfj"});
        paramsList.add(new Object[] {"asdf2"});
        paramsList.add(new Object[] {"asdf4"});

        jdbcHelper.executeBatch(sql, paramsList);


    }
}