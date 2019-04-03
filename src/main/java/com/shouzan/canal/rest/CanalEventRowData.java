package com.shouzan.canal.rest;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.alibaba.otter.canal.protocol.CanalEntry.RowData;
import com.shouzan.canal.biz.CanalBiz;
import com.shouzan.canal.entity.LogEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/18 15:35
 * @Description:
 */
@Component
public class CanalEventRowData {

    @Autowired
    private CanalBiz canalBiz;

    /**
     * @Description: (增删改日志信息处理)
     * @param eventType
     * @param rowChage
     * @param logEntity
     * @[param] [eventType, rowChage, logEntity]
     * @return void
     * @author:  bin.yang
     * @date:  2019/3/18 3:46 PM
     */
    public void publishCanalRowData(CanalEntry.EventType eventType, RowChange rowChage, LogEntity logEntity){
        for (RowData rowData : rowChage.getRowDatasList()) {

            //将Column 转换至 MAP 集合
            Map<String, String> iMap = ColumnToMap(rowData.getAfterColumnsList());
            logEntity.fields(iMap);

            //分流处理
            switch (eventType) {
                case INSERT:
                    canalBiz.InsertCanalEvent(logEntity);
                    break;
                case UPDATE:
                    canalBiz.UpdateCanalEvent(logEntity);
                    break;
                case DELETE:
                    canalBiz.DeleteCanalEvent(logEntity);
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * @Description: (数据转换)
     * @param columns
     * @[param] [columns]
     * @return java.util.HashMap<java.lang.String,java.lang.Object>
     * @author:  bin.yang
     * @date:  2019/3/15 1:05 PM
     */
    public  Map<String, String> ColumnToMap(List<CanalEntry.Column> columns) {

        //将数据转换成MAP对象
        Map<String, String> map = new HashMap<>();
        if (columns != null && columns.size() > 0) {
            for (CanalEntry.Column column : columns) {
                map.put(column.getName(), column.getValue());
            }
        }
        return map;
    }
}
