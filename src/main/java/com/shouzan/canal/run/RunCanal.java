package com.shouzan.canal.run;

import com.shouzan.canal.rest.CanalController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/14 17:36
 * @Description:
 */
@Component
@Order(value = 1)
public class RunCanal implements ApplicationRunner {

    @Autowired
    private CanalController canalController;

    @Override
    public void run(ApplicationArguments args) throws Exception {
        //启动监听
        canalController.CreateClient();
    }
}