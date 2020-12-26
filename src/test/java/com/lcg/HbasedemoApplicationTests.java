package com.lcg;

import com.lcg.config.HbaseConfig;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Table;
import org.junit.jupiter.api.Test;
import org.junit.validator.PublicClassValidator;
import org.springframework.boot.test.context.SpringBootTest;

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

}
