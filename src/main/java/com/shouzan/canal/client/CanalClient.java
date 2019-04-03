package com.shouzan.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.shouzan.canal.constant.Concom;
import com.shouzan.canal.rest.CanalController;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/14 17:41
 * @Description:
 */
@Component
@Slf4j
public class CanalClient {

    @Autowired
    private CanalController controller;

    @Value("${canal.server.zkServers}")
    private String zkServers;

    @Value("${canal.server.subscribe}")
    private String subscribe;

    @Value("${canal.server.destination}")
    private String destination;

    @Value("${canal.server.refreshSeconds}")
    private long refreshSeconds;

    @Value("${canal.server.dbname}")
    private String dbname;

    @Value("${canal.server.username}")
    private String username;

    @Value("${canal.server.password}")
    private String password;

    private CanalConnector connector;

    private long batchId = 0;

    /**
     * @Description: (创建zookeeper连接 , 发送日志请求)
     * @param
     * @[param] []
     * @return void
     * @author:  bin.yang
     * @date:  2019/3/15 12:49 PM
     */
    public void connectCanal() throws InterruptedException {

        // 外层死循环： 在canal节点宕机后，抛出异常，
        // 等待zookeeper对canal处理机的切换，切换完后，继续创建连接处理数据
        while (true){
            if(null==connector || !connector.checkValid()){
                try {
                    connector = CanalConnectors.newClusterConnector(zkServers, destination, username, password);
                    connector.connect();
//                    connector.subscribe(subscribe);
                    connector.subscribe(".*\\..*");  // 监控所有表所有库
                    log.debug(">>>> Connection canal server successful,zkServers：【{}】,subscribe：【{}】 <<<<<",zkServers,subscribe);
                } catch (Exception e) {
                    log.debug(">>>> Connection canal server failed,zkServers：【{}】,subscribe：【{}】, exception：【{}】...try again after 10s <<<<<",zkServers,subscribe,e.getMessage());
                    Thread.sleep(Concom.EXCEPTION_SECONDS);
                    continue;
                }
            }

            // 内层死循环: 按频率实时监听数据变化，
            // 一旦收到变化数据，立即做消费处理，并ack
            try {
                while (true) {
                    try {
                        // 获取指定数量的数据
                        Message messages = connector.getWithoutAck(Concom.CANAL_BATCH_SIZE);
                        if(null == messages){
                            log.debug(">>>> Canal client connect zookeeper server is running, get Message is null!");
                            Thread.sleep(Concom.EXCEPTION_SECONDS);
                            continue;
                        }

                        //获取同步id
                        batchId = messages.getId();
                        int size = messages.getEntries().size();
                        if (batchId == -1 || size == 0) {
                            try {
                                Thread.sleep(Concom.MAX_SLEEP_SECONDS);
                                connector.ack(batchId);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            log.info(">>>> Canal client No DATA change ! batchId={}  <<<<<", batchId);
                        } else {
                            //处理信息
                            controller.publishCanalListEvent(messages.getEntries());
                            connector.ack(batchId);
                            log.info(">>>> Canal client consume canal  {}   is  success!!!  <<<<<", batchId);
                        }
                    }catch (Exception e){
                        log.error("read canal message error , exception : ", e);
                        // 处理失败, 按偏移量回滚数据
                        connector.rollback(batchId);
                    }

                }
            } catch (CanalClientException e) {
                //关闭连接
                if(connector != null){
                    connector.disconnect();
                }
                log.info(">>>> Canal client server canal  {}   is  close!!!  <<<<<", new Date().toLocaleString());
            }
        }

    }

}
