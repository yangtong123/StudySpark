package com.yt.spark.dao.factory;

import com.yt.spark.dao.*;
import com.yt.spark.dao.impl.*;

/**
 * Created by yangtong on 17/5/8.
 */
public class DAOFactory {

    public static ITaskDAO getTaskDAO() {
        return new TaskDAOImpl();
    }

    public static ISessionAggrStatDAO getSessionAggrStatDAO() {
        return new SessionAggrStatDAOImpl();
    }

    public static ISessionRandomExtractDAO getSessionRandomExtractDAO() {
        return new SessionRandomExtractDAOImpl();
    }

    public static ISessionDetailDAO getSessionDetailDAO() {
        return new SessionDetailDAOImpl();
    }

    public static ITop10CategoryDAO getTop10CategoryDAO() {
        return new Top10CategoryDAOImpl();
    }

    public static ITop10SessionDAO getTop10SessionDAO() {
        return new Top10SessionDAOImpl();
    }

    public static IPageSplitConvertRateDAO getPageSplitConvertRateDAO() {
        return new PageSplitConvertRateDAOImpl();
    }

    public static IAreaTop3ProductDAO getAreaTop3ProductDAO() {
        return new AreaTop3ProductDAOImpl();
    }

    public static IAdUserClickCountDAO getAdUserClickCountDAO() {
        return new AdUserClickCountDAOImpl();
    }

    public static IAdBlacklistDAO getAdBlacklistDAO() {
        return new AdBlacklistDAOImpl();
    }

    public static IAdStatDAO getAdStatDAO() {
        return new AdStatDAOImpl();
    }

    public static IAdProvinceTop3DAO getAdProvinceTop3DAO() {
        return new AdProvinceTop3DAOImpl();
    }

    public static  IAdClickTrendDAO getAdClickTrendDAO() {
        return new AdClickTrendDAOImpl();
    }
}
