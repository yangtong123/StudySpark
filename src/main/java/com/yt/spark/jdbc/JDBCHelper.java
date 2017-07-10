package com.yt.spark.jdbc;

import com.yt.spark.conf.Configuration;
import com.yt.spark.conf.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * JDBC辅助组件
 *
 * Created by yangtong on 17/5/5.
 */
public class JDBCHelper {

    //加载数据库驱动
    static {
        try {
            String driver = Configuration.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    //实现单例化
    private static JDBCHelper instance = null;

    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    instance = new JDBCHelper();
                }
            }
        }
        return instance;
    }

    private LinkedList<Connection> datasources = new LinkedList<Connection>();

    private JDBCHelper() {
        int datasourceSize = Configuration.getInteger(Constants.JDBC_DATASOURCE_SIZE);

        boolean local = Configuration.getBoolean(Constants.SPARK_LOCAL);
        String url = null;
        String user = null;
        String password = null;

        if (local) {
            url = Configuration.getProperty(Constants.JDBC_URL);
            user = Configuration.getProperty(Constants.JDBC_USER);
            password = Configuration.getProperty(Constants.JDBC_PASSWORD);
        } else {
            url = Configuration.getProperty(Constants.JDBC_URL_PROD);
            user = Configuration.getProperty(Constants.JDBC_USER_PROD);
            password = Configuration.getProperty(Constants.JDBC_PASSWORD_PROD);
        }

        //创建指定数量的数据库连接，并放入数据库连接池中
        for (int i = 0; i < datasourceSize; i++) {
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                datasources.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    /**
     * 提供获取数据库连接的方法，并做多线程访问控制
     * @return
     */
    public synchronized Connection getConnection() {
        //如果连接池为空，那么等待1秒
        while (datasources.size() == 0) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return datasources.poll();
    }

    /**
     * 增删改查
     * @param sql
     * @param params
     * @return 影响的行数
     */
    public int executeUpdate(String sql, Object[] params) {
        int rtn = 0;

        Connection conn = null;
        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i+1, params[i]);
                }
            }

            rtn = pstmt.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasources.push(conn);
            }
        }

        return rtn;
    }

    /**
     * 执行查询语句
     * @param sql
     * @param params
     * @param callBack
     */
    public void executeQuery(String sql, Object[] params, QueryCallBack callBack) {
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;

        try {
            conn = getConnection();
            pstmt = conn.prepareStatement(sql);

            if (params != null && params.length > 0) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i+1, params[i]);
                }
            }

            rs = pstmt.executeQuery();

            callBack.process(rs);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasources.push(conn);
            }
        }
    }

    /**
     * 批量执行sql语句
     * @param sql
     * @param paramsList
     * @return
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) {
        int[] rtn = null;
        Connection conn = null;

        PreparedStatement pstmt = null;

        try {
            conn = getConnection();
            //1.取消自动提交
            conn.setAutoCommit(false);
            pstmt = conn.prepareStatement(sql);

            if (paramsList != null && paramsList.size() > 0) {
                //2.使用PreparedStatement.addBatch()方法加入批量sql操作
                for (Object[] params : paramsList) {
                    for (int i = 0; i < params.length; i++) {
                        pstmt.setObject(i+1, params[i]);
                    }
                    pstmt.addBatch();
                }
            }

            //3.使用PreparedStatement.executeBatch()执行批量
            rtn = pstmt.executeBatch();

            //4.批量提交
            conn.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                datasources.push(conn);
            }
        }


        return rtn;
    }


    /**
     * 查询回调接口
     */
    public static interface QueryCallBack {
        /**
         * 处理查询结果
         * @param rs
         * @throws Exception
         */
        void process(ResultSet rs) throws Exception;
    }
}
