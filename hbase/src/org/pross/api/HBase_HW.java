package org.pross.api;

import java.io.IOException;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.xml.bind.v2.runtime.property.ValueProperty;

import io.netty.handler.codec.http.HttpHeaders.Values;
import jdk.internal.org.objectweb.asm.tree.analysis.Value;


/**
 * @author pross shawn
 *
 * create time：2018年3月29日
 *
 * content：hbase api 使用
 */
public class HBase_HW implements HBaseDemoInterface {
	private static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
	private static final String ZK_CONNECT_VALUE = "hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181";
	private static Connection con = null;
	private static Admin admin = null;
	private static Table table = null;
	private static Configuration conf = null;
	
	
	public static void main(String[] args) throws Exception {
		HBase_HW hb = new HBase_HW();
		hb.hbaseCon();
		
//		hb.getAllTables();
		
//		String[] family={"col1","col2"};
//		hb.createTable("test", family);
		
//		hb.descTable("user_info");
		
		hb.existTable("user_info");
		
		hb.hbaseClose();
		
	}
	/*
	 * 初始化
	 */
	public void hbaseCon(){
		conf = HBaseConfiguration.create();
		conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
		try {
			con = ConnectionFactory.createConnection(conf);
			admin = con.getAdmin();
		} catch (IOException e) {
			System.out.println("连接hbase失败");
		}
		
	}
	/*
	 * 关闭
	 */
	public void hbaseClose() throws Exception{
		admin.close();
		con.close();
	}
	
	/*
	 * 查询所有表
	 */
	@Override
	public void getAllTables() throws Exception {
		//得到所有的表
		HTableDescriptor[] listTables = admin.listTables();
//		System.out.println(listTables);
		//遍历
		for(HTableDescriptor htd:listTables){
			System.out.println(htd.getTableName()+"\t");
			//得到表中的所有的列簇集合
			HColumnDescriptor[] columnFamilies = htd.getColumnFamilies();
			//遍历
			for(HColumnDescriptor hcd:columnFamilies){
				//得到表中所有的列簇Name
				System.out.print(Bytes.toString(hcd.getName())+",");
			}
			System.out.println();
		}
		
	}
	
	/*
	 * 创建表，传参，表名和列簇的名字
	 * HColumnDescriptor
	 */
	@Override
	public void createTable(String tableName, String[] family) throws Exception {
		TableName name = TableName.valueOf(tableName);
		HTableDescriptor htd = new HTableDescriptor(name);
		for(int i=0;i<family.length;i++){
			HColumnDescriptor hcd = new HColumnDescriptor(family[i]);
			htd.addFamily(hcd);	
		}
		admin.createTable(htd);
		
		if(admin.tableExists(name)){
			System.out.println("创建表成功");
		}else{
			System.out.println("创建表失败");			
		}
		
	}
	
	/*
	 * 创建表，传参：封装好的多个列簇
	 */
	@Override
	public void createTable(HTableDescriptor htds) throws Exception {
		//得到表名
		TableName tableName = htds.getTableName();
		//创建表名
		HTableDescriptor newhtd= new HTableDescriptor(tableName);
		//得到列簇名
		HTableDescriptor htd = new HTableDescriptor(tableName);
		HColumnDescriptor[] columnFamilies = htd.getColumnFamilies();
		//遍历
		for(HColumnDescriptor hcd:columnFamilies){
			String colName = Bytes.toString(hcd.getName());
			HColumnDescriptor newhcd=new HColumnDescriptor(colName);
			newhtd.addFamily(newhcd);
		}
		//创建
		admin.createTable(newhtd);
		
		if(admin.tableExists(tableName)){
			System.out.println("创建表成功");
		}else{
			System.out.println("创建表失败");			
		}
		
	}
	
	/*
	 * 创建表，传参：表名和封装好的多个列簇
	 */
	@Override
	public void createTable(String tableName, HTableDescriptor htds) throws Exception {
		// TODO Auto-generated method stub
		
	}
	/*
	 *  查看表的列簇属性
	 */
	@Override
	public void descTable(String tableName) throws Exception {
		Scan scan = new Scan();
		ResultScanner rs = null;
		table = con.getTable(TableName.valueOf(tableName));
		
		rs=table.getScanner(scan);
		for(Result r:rs){
			//得到所有的rowkey，不重复
			System.out.println(Bytes.toString(r.getRow()));

//			for(KeyValue kv:r.list()){
//				//得到所有的rowkey
////				System.out.println("row"+Bytes.toString(kv.getRow()));
//				System.out.println(Bytes.toString());
//			}
		}
		
		
	}
	
	/*
	 * 判断表是否存在
	 */
	@Override
	public boolean existTable(String tableName) throws Exception {
		TableName tn=TableName.valueOf(tableName);
		if(admin.tableExists(tn)){
			System.out.println("存在");
			return true;
		}else{
			System.out.println("不存在");
			return false;
		}
	}
	
	/*
	 *  disable表
	 */
	@Override
	public void disableTable(String tableName) throws Exception {
		TableName tn=TableName.valueOf(tableName);
		//关闭表
		admin.disableTable(tn);
	}
	
	/*
	 * drop表
	 */
	@Override
	public void dropTable(String tableName) throws Exception {
		TableName tn=TableName.valueOf(tableName);
		//先关闭表，然后删除
		admin.disableTable(tn);
		admin.deleteTable(tn);
	}
	
	/*
	 * 添加表
	 */
	@Override
	public void modifyTable(String tableName) throws Exception {
		TableName tn=TableName.valueOf(tableName);
		HTableDescriptor htd=new HTableDescriptor(tn);
		admin.createTable(htd);
		
	}
	/*
	 * 创建表，添加列簇，删除列簇
	 */
	@Override
	public void modifyTable(String tableName, String[] addColumn, String[] removeColumn) throws Exception {
		TableName tn=TableName.valueOf(tableName);
		HTableDescriptor htd=new HTableDescriptor(tn);
		for(int i=0;i<addColumn.length;i++){
			//把列簇添加到表中
			HColumnDescriptor hcd=new HColumnDescriptor(addColumn[i]);
			htd.addFamily(hcd);
		}
		//创建表和表中的列簇
		if(admin.tableExists(tn)){
			System.exit(0);
		}else{
			admin.createTable(htd);
		}
		
		//删除表中的列簇
		admin.disableTable(tn);
		HTableDescriptor htdRemove=new HTableDescriptor(tn);
		for(int i=0;i<removeColumn.length;i++){
			htdRemove.removeFamily(Bytes.toBytes(removeColumn[i]));
		}
		
	}
	
	/*
	 * 创建表和封装好的列簇
	 */
	@Override
	public void modifyTable(String tableName, HColumnDescriptor hcds) throws Exception {
		//首先得到tablename对象
		TableName tn=TableName.valueOf(tableName);		
		HTableDescriptor htd=new HTableDescriptor(tn);
		htd.addFamily(hcds);
		//如果存在表则退出
		if(admin.tableExists(tn)){
			System.out.println("表已存在");
			System.exit(0);
		}else{
			admin.createTable(htd);
		}
	}
	
	
	/*
	 * 往某张表中  添加或者修改数据
	 */
	@Override
	public void addData(String tableName, String rowKey, String[] column, String[] value) throws Exception {
		table = con.getTable(TableName.valueOf(tableName));
		Put put=new Put(Bytes.toBytes(rowKey));
		for(int i=0;i<column.length;i++){
			put.addColumn(column[i].getBytes(), "xx".getBytes(), value[i].getBytes());
			table.put(put);
		}
	}
	
	@Override
	public void putData(String tableName, String rowKey, String familyName, String columnName, String value)
			throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void putData(String tableName, String rowKey, String familyName, String columnName, String value,
			long timestamp) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void putData(Put put) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void putData(List<Put> putList) throws Exception {
		// TODO Auto-generated method stub
		
	}
	
	/*
	 * 根据rowkey查询数据
	 */
	@Override
	public Result getResult(String tableName, String rowKey) throws Exception {
		table=con.getTable(TableName.valueOf(tableName));
		Get get=new Get(rowKey.getBytes());
		Result result = table.get(get);
		List<Cell> listCells = result.listCells();
		//遍历需要
//		for(Cell c:listCells){
//			System.out.println(c.getFamily());
//		}
		//直接返回
		return result;
	}
	@Override
	public Result getResult(String tableName, String rowKey, String familyName) throws Exception {
		TableName tn=TableName.valueOf(tableName);
		table=con.getTable(tn);
		Get get=new Get(rowKey.getBytes());
		get.addFamily(familyName.getBytes());
		table.get(get);
		return null;
	}
	@Override
	public Result getResult(String tableName, String rowKey, String familyName, String columnName) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public Result getResultByVersion(String tableName, String rowKey, String familyName, String columnName,
			int versions) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	//scan用法，全表扫描
	@Override
	public ResultScanner getResultScann(String tableName) throws Exception {
		TableName tn=TableName.valueOf(tableName);
		table=con.getTable(tn);
		Scan scan = new Scan();
		ResultScanner scanner = table.getScanner(scan);
		return scanner;
	}
	@Override
	public ResultScanner getResultScann(String tableName, Scan scan) throws Exception {
		// TODO Auto-generated method stub
		return null;
	}
	
	/* 
	 * 删除数据(指定的列)
	 */
	@Override
	public void deleteColumn(String tableName, String rowKey) throws Exception {
		// TODO Auto-generated method stub
		
	}
	@Override
	public void deleteColumn(String tableName, String rowKey, String falilyName) throws Exception {
		table=con.getTable(TableName.valueOf(tableName));
		Delete delete=new Delete(rowKey.getBytes());
		delete.addFamily(falilyName.getBytes());
		if(admin.isTableEnabled(TableName.valueOf(tableName))){
			admin.disableTable(TableName.valueOf(tableName));
			table.delete(delete);
		}
	}
	@Override
	public void deleteColumn(String tableName, String rowKey, String falilyName, String columnName) throws Exception {
		// TODO Auto-generated method stub
		
	}


}
