package com.aura.dao.impl;

import com.aura.bean.CategoryClickCount;
import com.aura.dao.HBaseDao;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HBaseImpl  implements HBaseDao{
    HConnection htablePool=null;
    public HBaseImpl(){
        Configuration conf = HBaseConfiguration.create();
        //HBase自带的zookeeper
        conf.set("hbase.zookeeper.quorum","hadoop1");
        conf.set("zookeeper.znode.parent","/mybase");
        try {
            htablePool=HConnectionManager.createConnection(conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 根据表名获取表对象
     * @param tableName  表名
     * @return 表对象
     */
    public HTableInterface getTable(String tableName){
        HTableInterface table=null;
        try {
            table = htablePool.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    /**
     * 往hbase里面插入一条数据
     * @param tableName 表名
     * @param rowkey  rowkey
     * @param family  列簇
     * @param q  品类
     * @param value  出现了多少次
     * 2018-12-12_电影 f q 15
     *        updateStateBykey 对内存要求要一点
     *         reduceBykey  对内存要求低一点
     * hbase: 只有一种数据类型，字节数组
     *
     */
    public void save(String tableName, String rowkey, String family, String q, long value) {
        HTableInterface table = getTable(tableName);

        try {
            table.incrementColumnValue(rowkey.getBytes(), family.getBytes(), q.getBytes(),
                     value);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
               if(table != null){
                   table.close();
               }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 根据rowkey返回数据
     * @param tableName  表名
     * @param rowkey rowkey
     * @return
     */
    public List<CategoryClickCount> count(String tableName, String rowkey) {
        ArrayList<CategoryClickCount> list = new ArrayList<CategoryClickCount>();
        HTableInterface table = getTable(tableName);
        PrefixFilter prefixFilter = new PrefixFilter(rowkey.getBytes());
        Scan scan = new Scan();
        scan.setFilter(prefixFilter);
        try {
            ResultScanner scanner = table.getScanner(scan);
            for(Result result:scanner){
               for(Cell cell: result.rawCells()){
                   byte[] date_name = CellUtil.cloneRow(cell);
                   String name = new String(date_name).split("_")[1];
                   byte[] value = CellUtil.cloneValue(cell);
                   long count = Bytes.toLong(value);
                   CategoryClickCount categoryClickCount = new CategoryClickCount(name, count);
                   list.add(categoryClickCount);
               }

            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
           if(table != null){
               try {
                   table.close();
               } catch (IOException e) {
                   e.printStackTrace();
               }
           }
        }
        return list;
    }
}
