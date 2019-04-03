package com.shouzan.canal.biz.impl;

import com.shouzan.canal.biz.CanalBiz;
import com.shouzan.canal.entity.LogEntity;
import org.springframework.stereotype.Component;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/15 12:54
 * @Description:
 */
@Component
public class CanalBizImpl implements CanalBiz {

//    @Autowired
//    private FeginElastic feginElastic;

    @Override
    public void InsertCanalEvent(LogEntity entity) {

        // fegin mq 业务处理吧
        System.out.println(entity);
    }

    @Override
    public void UpdateCanalEvent(LogEntity entity) {

        // fegin mq 业务处理吧
        System.out.println(entity);
    }

    @Override
    public void DeleteCanalEvent(LogEntity entity) {

        // fegin mq 业务处理吧
        System.out.println(entity);
    }


}
