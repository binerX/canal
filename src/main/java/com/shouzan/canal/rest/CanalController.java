package com.shouzan.canal.rest;

import com.alibaba.otter.canal.protocol.CanalEntry.Entry;
import com.alibaba.otter.canal.protocol.CanalEntry.EntryType;
import com.alibaba.otter.canal.protocol.CanalEntry.EventType;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.shouzan.canal.client.CanalClient;
import com.shouzan.canal.entity.LogEntity;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/14 17:59
 * @Description:
 */
@Component
@Slf4j
public class CanalController {

    @Autowired
    private CanalClient canalClient;

    @Autowired
    private CanalEventType canalEventType;

    @Autowired
    private CanalEventRowData canalEventRowData;

    /**
     * @Description: (创建canal连接)
     * @param
     * @[param] []
     * @return void
     * @author:  bin.yang
     * @date:  2019/3/15 12:51 PM
     */
    public void CreateClient(){
        try {
            canalClient.connectCanal();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * @Description: (日志信息处理)
     * @param entries
     * @[param] [entries]
     * @return void
     * @author:  bin.yang
     * @date:  2019/3/15 4:20 PM
     */
    public void publishCanalListEvent(List<Entry> entries) {
        for (Entry entry : entries) {

            //事物过滤
            if (entry.getEntryType().equals(EntryType.TRANSACTIONBEGIN)
                    || entry.getEntryType().equals(EntryType.TRANSACTIONEND)
                    || EventType.QUERY.equals(entry.getHeader().getEventType())) {
                continue;
            }

            //解析日志信息
            RowChange rowChage = null;
            try {
                rowChage = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            //打印BINLOG日志信息
            System.err.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(), rowChage.getEventType()));

            //封装LOG实体
            LogEntity logEntity = new LogEntity()
                    .index(entry.getHeader().getSchemaName())
                    .eventType(rowChage.getEventType().toString())
                    .docType(entry.getHeader().getTableName());

            //操作日志
            publishCanalEvent(rowChage,logEntity);
        }
    }

    /**
     * @Description: (根据操作类型分流处理)
     * @param rowChage
     * @param logEntity
     * @[param] [rowChage, logEntity]
     * @return void
     * @author:  bin.yang
     * @date:  2019/3/15 6:16 PM
     */
    private void publishCanalEvent(RowChange rowChage, LogEntity logEntity) {

        //获取操作类型
        EventType eventType = rowChage.getEventType();

        //逻辑拆分处理
        switch (eventType) {
            case INSERT:
                canalEventRowData.publishCanalRowData(eventType,rowChage,logEntity);
                break;
            case UPDATE:
                canalEventRowData.publishCanalRowData(eventType, rowChage,logEntity);
                break;
            case DELETE:
                canalEventRowData.publishCanalRowData(eventType, rowChage,logEntity);
                break;
            case CREATE:
                canalEventType.publishCanalType(eventType, rowChage,logEntity);
                break;
            case ERASE:
                canalEventType.publishCanalType(eventType, rowChage,logEntity);
                break;
            case ALTER:
                canalEventType.publishCanalType(eventType, rowChage,logEntity);
                break;
            default:
                break;
        }
    }

}
