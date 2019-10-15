package com.wang.utils;

import com.wang.entity.Course;
import com.wang.entity.Student;
import javafx.scene.control.Tab;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
@Slf4j
public class HbaseUtils {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    private HbaseUtils(){}
    static {
        configuration=HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.zookeeper.quorum","127.0.0.1");
        configuration.set("hbase.master", "127.0.0.1:60000");
        try {
            connection= ConnectionFactory.createConnection(configuration);
            admin=connection.getAdmin();
            log.info("创建连接成功!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建表
     * @param tableName 表名
     * @param cols 列族
     */
    public static void createTable(String tableName,String [] cols){
        try {
            if(tableIsExists(tableName)){
                log.error("表已经存在!");
                return;
            }
            HTableDescriptor hTableDescriptor=new HTableDescriptor(TableName.valueOf(tableName));
            for(String col:cols){
                HColumnDescriptor hColumnDescriptor=new HColumnDescriptor(col);
                hTableDescriptor.addFamily(hColumnDescriptor);
            }
            admin.createTable(hTableDescriptor);
            log.info("建表成功!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入数据
     * @param tableName
     * @param student
     */
    public static void insert(String tableName, Student student){
        TableName table=TableName.valueOf(tableName);
        try {
            if(!admin.tableExists(table))
                throw new Error("表不存在!");
            Put put=new Put(student.getS_no().getBytes());
            put.addColumn("Info".getBytes(),"Sname".getBytes(),student.getName().getBytes());
            put.addColumn("Info".getBytes(),"Ssex".getBytes(),student.getSex().getBytes());
            for(Course c: student.getCourses()){
                put.addColumn("Course".getBytes(),"Cname".getBytes(),c.getName().getBytes());
                put.addColumn("Course".getBytes(),"Cscore".getBytes(),c.getScore().getBytes());
                Table t=connection.getTable(TableName.valueOf(tableName));
                t.put(put);
            }
            log.info("插入成功："+student.toString());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取原始数据
     * @param tableName
     */
    public static void getNoDealData(String tableName){
        if(!tableIsExists(tableName))
            throw new Error("表不存在");
        try {
            Table table=connection.getTable(TableName.valueOf(tableName));
            Scan scan=new Scan();
            ResultScanner resultScanner=table.getScanner(scan);
            for(Result rs : resultScanner){
                log.info("scan:"+String.valueOf(rs));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
    public static void getDataByRowKey(String tableName,String rowKey){
        if(!tableIsExists(tableName))
            throw new Error("表不存在");
        try {
            Table table=connection.getTable(TableName.valueOf(tableName));
            Get get=new Get(rowKey.getBytes());

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除表
     * @param tableName
     */
    public static void delete(String tableName){
        if (!tableIsExists(tableName)){
            log.error("表不存在");
            return;
        }
        try {
            TableName table=TableName.valueOf(tableName);
            admin.disableTable(table);
            admin.deleteTable(table);
            log.info("删除表:"+tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static boolean tableIsExists(String tableName){
        TableName table=TableName.valueOf(tableName);
        try {
            return admin.tableExists(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }
}
