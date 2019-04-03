package com.shouzan.canal.entity;

import lombok.Data;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/1 15:37
 * @Description:
 */
@Data
public class LogEntity implements Serializable {

    private static final long serialVersionUID = -2340949926536837921L;

    // 索引 > 数据库
    private String index;

    // 索引类型 > 表
    private String docType;

    // 操作类型
    private String eventType;

    // 数据集合 新增修改删除数据集
    private Map<String,String> fields;

    // Type集合 新增删除type
    private List<Map<String, String>> objects;

    // DDL集合  Alter操作数据集
    private Map<String , Object> operates;

    public LogEntity index(String index) {
        this.setIndex(index);
        return this;
    }

    public LogEntity docType(String docType) {
        this.setDocType(docType);
        return this;
    }

    public LogEntity eventType(String eventType) {
        this.setEventType(eventType);
        return this;
    }

    public LogEntity fields(Map<String,String> map) {
        this.setFields(map);
        return this;
    }

    public LogEntity objects(List<Map<String, String>>  list) {
        this.setObjects(list);
        return this;
    }

    public LogEntity operates(Map<String , Object> map) {
        this.setOperates(map);
        return this;
    }
}
