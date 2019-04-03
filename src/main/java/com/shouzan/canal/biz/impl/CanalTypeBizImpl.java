package com.shouzan.canal.biz.impl;

import com.shouzan.canal.biz.CanalTypeBiz;
import com.shouzan.canal.entity.LogEntity;
import com.shouzan.canal.rpc.FeginElastic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/18 15:42
 * @Description:
 */
@Component
public class CanalTypeBizImpl implements CanalTypeBiz {

//    @Autowired
//    private FeginElastic feginElastic;

    @Override
    public void DropCanalEvent(LogEntity entity) {

        // fegin mq 业务处理吧
        System.out.println(entity);
    }

    @Override
    public void CreateCanalEvent(LogEntity entity) {

        // fegin mq 业务处理吧
        System.out.println(entity);
    }

    @Override
    public void AlterCanalEvent(LogEntity entity) {

        // fegin mq 业务处理吧
        System.out.println(entity);
    }
}
