package com.yt.spark.dao;

import com.yt.spark.domain.Task;

/**
 * 任务管理DAO接口
 *
 * Created by yangtong on 17/5/8.
 */
public interface ITaskDAO {

    /**
     * 根据主键查询接口
     * @param taskId
     * @return Task对象
     */
    Task findById(long taskId);


}
