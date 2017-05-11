package com.yt.spark.dao.impl;

import com.yt.spark.dao.ISessionAggrStatDAO;
import com.yt.spark.dao.ISessionDetailDAO;
import com.yt.spark.dao.ISessionRandomExtractDAO;
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

    /**
     * 获得SessionAggrStatDAO
     * @return
     */
    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailDAOImpl();
    }
}
