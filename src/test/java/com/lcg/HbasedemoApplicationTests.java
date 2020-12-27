package com.lcg;

import com.lcg.config.HbaseConfig;
import com.lcg.util.HBaseApi;
import org.apache.hadoop.hbase.client.Connection;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.Test;
import org.junit.validator.PublicClassValidator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;
import java.io.IOException;

@SpringBootTest
class HbasedemoApplicationTests {

    @Test
    public void getConnTest(){
        Connection connection= HbaseConfig.getHBaseConnection();
        System.out.println(connection.isClosed());
        HbaseConfig.closeConn();
        System.out.println(connection.isClosed());
    }

    @Test
    public void getTable(){
        Table table=null;
        try {
            table=HbaseConfig.getTable("fruit");
            System.out.println(table.getTableDescriptor());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //测试创建表
    @Test
    public void createTable(){
        HBaseApi hBaseApi = new HBaseApi();
        try {
            String []str={"info1","info2"};
            hBaseApi.createTable("NewPerson",str);
            System.out.println("创建表成功");
        } catch (IOException e) {
            System.out.println("建表失败");
            e.printStackTrace();
        }
    }

    @Test
    public void scanner ()throws IOException {
        HBaseApi hBaseApi = new HBaseApi();
        ResultScanner res= hBaseApi.getScanner("fruit");
        Result s;
        while((s=res.next())!=null){
            System.out.println(s.cellScanner());
        }
    }

    @Test
    public void deleteRow() throws IOException {
        HBaseApi hBaseApi = new HBaseApi();
        if(hBaseApi.deleteRow("fruit","1001")){
            System.out.println("删除成功");
        }else {
            System.out.println("删除失败");
        }
    }

    @Test
    public void deleteCf() throws IOException {
        HBaseApi hBaseApi = new HBaseApi();
        if(hBaseApi.deleteCf("NewPerson","info1")){
            System.out.println("删除成功");
        }else{
            System.out.println("删除失败");
        }
    }

    @Test
    public void deleteQua() throws IOException {
        HBaseApi hBaseApi = new HBaseApi();
        if(hBaseApi.deleteQua("fruit","1002","info","color")){
            System.out.println("删除成功r");
        }else{
            System.out.println("删除失败");
        }
    }
}
