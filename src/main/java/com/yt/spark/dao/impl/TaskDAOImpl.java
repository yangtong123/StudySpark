package com.yt.spark.dao.impl;

import com.yt.spark.dao.ITaskDAO;
import com.yt.spark.domain.Task;
import com.yt.spark.jdbc.JDBCHelper;

import java.sql.ResultSet;

/**
 * 任务管理DAO实现类
 *
 * Created by yangtong on 17/5/8.
 */
public class TaskDAOImpl implements ITaskDAO {
    /**
     * 根据主键查询任务
     * @param taskId Task主键
     * @return Task
     */
    @Override
    public Task findById(long taskId) {
        final Task task = new Task(); //jdk8 可以不用final，会自动变成final

        String sql = "select * from task where task_id = ?";
        Object[] params = new Object[] {taskId};

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeQuery(
                sql,
                params,
                new JDBCHelper.QueryCallBack() {
                    @Override
                    public void process(ResultSet rs) throws Exception {
                        if (rs.next()) {
                            long taskId = rs.getLong(1);
                            String taskName = rs.getString(2);
                            String createTime = rs.getString(3);
                            String startTime = rs.getString(4);
                            String finishTime = rs.getString(5);
                            String taskType = rs.getString(6);
                            String taskStatus = rs.getString(7);
                            String taskParams = rs.getString(8);

                            task.setTaskId(taskId);
                            task.setTaskName(taskName);
                            task.setCreateTime(createTime);
                            task.setStartTime(startTime);
                            task.setFinishTime(finishTime);
                            task.setTaskType(taskType);
                            task.setTaskStatus(taskStatus);
                            task.setTaskParam(taskParams);
                        }
                    }
                });

        return task;
    }
}
