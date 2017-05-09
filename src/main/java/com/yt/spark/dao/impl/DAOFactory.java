package com.yt.spark.dao.impl;

import com.yt.spark.dao.ITaskDAO;

/**
 * Created by yangtong on 17/5/8.
 */
public class DAOFactory {
    /**
     * 使用工厂模式来获取TaskDAO
     * @return
     */
    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }
}
