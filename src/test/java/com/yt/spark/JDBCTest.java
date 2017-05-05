package com.yt.spark;

import java.sql.*;

/**
 * Created by yangtong on 17/5/5.
 */
public class JDBCTest {
    public static void main(String[] args) throws SQLException {
        insert();
    }

    public static void insert() throws SQLException {
        Connection conn = null;

        Statement stmt = null;

        PreparedStatement pstmt = null;

        try {
            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/spark_project",
                    "root",
                    "542919899");

            stmt = conn.createStatement();


            String sql = "insert into test_jdbc(name) values('yt')";
            String pSql = "insert into test_jdbc(name) values(?)";
//            int a = stmt.executeUpdate(sql);

            pstmt = conn.prepareStatement(pSql);
            pstmt.setString(1, "yangtong");

            int b = pstmt.executeUpdate();

            System.out.println(b);
        } catch (Exception e) {

        } finally {
            if (stmt != null) {
                stmt.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }
}
