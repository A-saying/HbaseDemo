package com.lcg.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;


//主要是用来配置hbase的连接
public class HbaseConfig {
    private static final HbaseConfig INSTANCE=new HbaseConfig();
    private static Configuration configuration;
    private static Connection connection;

    private HbaseConfig(){
        if(configuration==null){
            configuration= HBaseConfiguration.create();
            configuration.set("hbase.zookeeper.quorum","192.168.140.132:2181");
        }
    }

    private  Connection getConnection(){
        if(connection==null||connection.isClosed()){
            try {
                connection= ConnectionFactory.createConnection(configuration);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static Connection getHBaseConnection(){
        return INSTANCE.getConnection();
    }

    public static Table getTable(String tableName)throws IOException{
        return  INSTANCE.getConnection().getTable(TableName.valueOf(tableName));
    }

    public static void  closeConn(){
        if(connection!=null){
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
