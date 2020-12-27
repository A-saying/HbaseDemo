package com.lcg.util;

import com.lcg.config.HbaseConfig;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.Op;
import sun.net.idn.Punycode;

import java.io.IOException;
import java.util.List;

public class HBaseApi {
    /*
    * 对表的结构的操作使用admin，对表内容的操作使用table
    * */
    //创建表
    public Boolean createTable(String tableName, String[]args) throws IOException {
        HBaseAdmin admin = (HBaseAdmin) HbaseConfig.getHBaseConnection().getAdmin();
        //判断表是否已经存在
        if (admin.tableExists(tableName)) {
            System.out.println("表已经存在");
            return false;
        }
        //不存在，则创建
        HTableDescriptor hTableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        //添加列簇
        for (int i = 0; i < args.length; i++) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(args[i]);
            hColumnDescriptor.setMaxVersions(1);
            hTableDescriptor.addFamily(hColumnDescriptor);
        }
        admin.createTable(hTableDescriptor);
        return true;
    }

    /*
     *  插入行,put 'tableName','rowKey','cfName:qualifier','value'
     */
    public boolean putRow(String tableName, String rowKey, String cfName, String qualfier, String value) throws IOException {
        Table table = HbaseConfig.getTable(tableName);
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(cfName), Bytes.toBytes(qualfier), Bytes.toBytes(value));
        table.put(put);
        return true;
    }

    /*
     *   批量插入
     * */
    public boolean putRows(String tableName, List<Put> puts) throws IOException {
        Table table = HbaseConfig.getTable(tableName);
        table.put(puts);
        return true;
    }

    //查询数据
    public Result getRow(String tableName, String rowKey) throws IOException {
        Table table = HbaseConfig.getTable(tableName);
        Get get = new Get(Bytes.toBytes(rowKey));
        return table.get(get);
    }

    public static Result getRow(String tableName, String rowKey, FilterList filterList) {
        try (Table table = HbaseConfig.getTable(tableName)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            get.setFilter(filterList);
            return table.get(get);
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
        return null;
    }

    //全表查询
    public ResultScanner getScanner(String tableName) throws IOException {
        Table table=HbaseConfig.getTable(tableName);
        Scan scan =new Scan();
        scan.setCaching(1000);
        return table.getScanner(scan);
    }

    public ResultScanner getScanner(String tableName,String startRow,String endRow) throws IOException {
        Table table=HbaseConfig.getTable(tableName);
        Scan scan=new Scan();
        scan.setStartRow(Bytes.toBytes(startRow));
        scan.setStopRow(Bytes.toBytes(endRow));
        return table.getScanner(scan);
    }

    //删除行
    public boolean deleteRow(String tableName,String rowKey) throws IOException {
        Table table=HbaseConfig.getTable(tableName);
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
        return true;
    }

    //删除列簇
    public boolean deleteCf(String tableName,String cf) throws IOException {
        HBaseAdmin admin=(HBaseAdmin)HbaseConfig.getHBaseConnection().getAdmin();
        admin.deleteColumn(tableName,cf);
        return true;
    }

    //删除某行的列簇中某列,用table
    public boolean deleteQua(String tableName,String rowKey,String cf,String qua) throws IOException {
        Table table=HbaseConfig.getTable(tableName);
        Delete delete=new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(cf),Bytes.toBytes(qua));
        table.delete(delete);
        return true;
    }
}
