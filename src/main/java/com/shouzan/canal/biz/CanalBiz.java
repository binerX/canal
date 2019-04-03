package com.shouzan.canal.biz;

import com.shouzan.canal.entity.LogEntity;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/15 12:54
 * @Description:
 */
public interface CanalBiz {

    void InsertCanalEvent(LogEntity entity);

    void UpdateCanalEvent(LogEntity entity);

    void DeleteCanalEvent(LogEntity entity);


}
