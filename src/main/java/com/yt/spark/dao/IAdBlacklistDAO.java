package com.yt.spark.dao;

import com.yt.spark.domain.AdBlacklist;

import java.util.List;

/**
 * Created by yangtong on 17/5/22.
 */
public interface IAdBlacklistDAO {
    /**
     * 批量插入
     * @param adBlacklists
     */
    void  insertBatch(List<AdBlacklist> adBlacklists);

    /**
     * 查询所有广告黑名单用户
     * @return
     */
    List<AdBlacklist> findAll();
}
