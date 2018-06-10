package org.pross.util;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

public class HBaseUtil {
	
	private static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
	private static final String ZK_CONNECT_VALUE = "hadoop02:2181,hadoop03:2181,hadoop04:2181";


	public static Connection getConnection() {
		
		
		Configuration conf = HBaseConfiguration.create();
		conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
		Connection con = null;
		try{
			con = ConnectionFactory.createConnection(conf);
		}catch (Exception e){
			System.out.println("获取hbase连接失败");
		}
		return con;
	}
	
	public static Admin getAdmin(){
		Connection connection = getConnection();
		
		try {
			return connection.getAdmin();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static Table getTable(String name){
		Connection connection = getConnection();
		
		try {
			return connection.getTable(TableName.valueOf(name));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
