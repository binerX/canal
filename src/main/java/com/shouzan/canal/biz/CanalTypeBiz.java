package com.shouzan.canal.biz;

import com.shouzan.canal.entity.LogEntity;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/18 15:42
 * @Description:
 */
public interface CanalTypeBiz {

    void DropCanalEvent(LogEntity entity);

    void CreateCanalEvent(LogEntity entity);

    void AlterCanalEvent(LogEntity entity);

}
