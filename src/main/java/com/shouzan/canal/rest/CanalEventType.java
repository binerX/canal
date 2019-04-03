package com.shouzan.canal.rest;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import com.alibaba.druid.stat.TableStat.Column;
import com.alibaba.druid.util.JdbcConstants;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.shouzan.canal.biz.CanalTypeBiz;
import com.shouzan.canal.entity.LogEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;

/**
 * @Auther: bin.yang
 * @Date: 2019/3/18 15:35
 * @Description:
 */
@Component
public class CanalEventType {

    @Autowired
    private CanalTypeBiz canalTypeBiz;

    public void publishCanalType(CanalEntry.EventType eventType, RowChange rowChage, LogEntity logEntity){

        //对数据的DDL操作 进行拆分处理
        switch (eventType) {
            case CREATE:
                AlterSqlHandle(rowChage.getSql(),logEntity);
                break;
            case ERASE:
                canalTypeBiz.DropCanalEvent(logEntity);
                break;
            case ALTER:
                AlterSqlHandle(rowChage.getSql(),logEntity);
                break;
            default:
                break;
        }
    }

    /**
     * @Description: (Create Sql 解析)
     * @param sql
     * @param logEntity
     * @[param] [sql]
     * @return void
     * @author:  bin.yang
     * @date:  2019/3/18 6:01 PM
     */
    public void AlterSqlHandle(String sql, LogEntity logEntity) {
        String dbType = JdbcConstants.MYSQL;

        //格式化输出
        String result = SQLUtils.format(sql, dbType);

//        MySqlStatementParser sqlStatementParser = new MySqlStatementParser(result) ;
//        List<SQLStatement> stmtList = sqlStatementParser.parseStatementList();

        List<SQLStatement> stmtList = SQLUtils.parseStatements(sql, dbType);

        // 获取字段详细
        for (int i = 0; i < stmtList.size(); i++) {

            SQLStatement stmt = stmtList.get(i);
            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
            stmt.accept(visitor);

            //获取操作方法名称,依赖于表名称
            System.out.println("Tables : " + visitor.getTables());

            //获取字段名称
            System.out.println("fields : " + visitor.getColumns());

            //获取所有字段

            Collection<Column> columns = visitor.getColumns();

            if(result.indexOf("CREATE TABLE") > -1){

                //匹配字段类型
                List<Map<String, String>> objects = getColumnType(result, columns);
                System.err.println("Types : " + objects);
                canalTypeBiz.CreateCanalEvent(logEntity.objects(objects));
            }else {

                // 获取 Alter 操作字段
                Map<String , Object> operates = getDMLColumnOperate(result, columns);
                System.err.println("Types : " + operates);
                canalTypeBiz.AlterCanalEvent(logEntity.operates(operates));
            }
        }
    }

    public Map<String , Object> getDMLColumnOperate(String result, Collection<Column> columns){

        // 创建集合手机字段信息
        Map<String , Object> operates = new HashMap<>();
        List<Map<String, String>> add = new ArrayList<>();
        List<Map<String, String>> change = new ArrayList<>();
        List<String> drop = new ArrayList<>();
        Map<String, String> map;

        //处理SQL
        String[] split = result.split("\n\t");

        // 字段截取
        int count = 1;
        String ty = null;
        for (Column column : columns) {

            //去掉空格
            String trim = split[count].trim();

            // 新增字段
            if(split[count].indexOf("ADD") > -1){

                int i = trim.indexOf(column.getName());
                if(i >= 0){
                    String[] splitTrim = trim.split(" ");
                    ty = splitTrim[3];
                    if(ty.indexOf("(") > -1){
                        ty = ty.substring(0 ,ty.indexOf("("));
                    }
                }
                if(ty != null){
                    map = new HashMap<String, String>();
                    map.put("field",column.getName());
                    map.put("type",ty);
                    add.add(map);
                }

            //  字段修改
            }else if(split[count].indexOf("CHANGE") > -1){

                String na = null;

                int i = trim.indexOf(column.getName());
                if(i >= 0){
                    String[] splitTrim = trim.split(" ");
                    ty = splitTrim[4];
                    if(ty.indexOf("(") > -1){
                        ty = ty.substring(0 ,ty.indexOf("("));
                    }
                    na = splitTrim[3].replace("`" , "");
                }
                if(ty != null && na != null){
                    map = new HashMap<String, String>();
                    map.put("old" ,column.getName());
                    map.put("new" ,na);
                    map.put("type" ,ty);
                    change.add(map);
                }

            //删除字段
            }else {
                drop.add(column.getName());
            }
            count++;
        }
        operates.put("ADD" , add);
        operates.put("DROP" , drop);
        operates.put("change" , change);
        return operates;
    }

    /**
     * @Description: (获取字段类型)
     * @param result
     * @param columns
     * @[param] [result, columns]
     * @return com.alibaba.fastjson.JSONObject
     * @author:  bin.yang
     * @date:  2019/3/18 5:59 PMCollection<Column>
     */
    public List<Map<String , String>> getColumnType(String result, Collection<Column> columns){

        List<Map<String , String>> objects = new ArrayList<>();
        Map<String, String> map;

        //处理SQL
        String substring = result.substring(result.indexOf("("));
        String[] split = substring.split("\n\t");

        // 字段截取
        int count = 1;
        String ty = null;
        for (Column column : columns) {

            //去掉空格
            String trim = split[count].trim();

            int i = trim.indexOf(column.getName());
            if(i >= 0){
                String[] splitTrim = trim.split(" ");
                ty = splitTrim[1];
                if(ty.indexOf("(") > -1){
                    ty = ty.substring(0 ,ty.indexOf("("));
                }
            }
            if(ty != null){
                map = new HashMap<String, String>();
                map.put("field",column.getName());
                map.put("type",ty);
                objects.add(map);
            }
            count++;
        }
        return objects;
    }

}
