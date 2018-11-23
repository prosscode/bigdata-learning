package org.pross.api;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;


/**
 * @author pross shawn
 *
 * create time：2018年3月29日
 *
 * content：HBase 第一个程序
 */
public class HBase_First {


	public static void main(String[] args) throws IOException {
		/*
		 * 第一步：获取连接和admin
		 */
		Configuration config = HBaseConfiguration.create();
		String string = "hadoop02:2182,hadoop03:2181,hadoop04:2181,hadoop05:2181";
		config.set("hbase.zookeeper.quorum", string);
		Connection con = ConnectionFactory.createConnection(config);
		Admin admin = con.getAdmin();
		//判断表是否存在
		admin.tableExists(TableName.valueOf("user_info"));

	}

}
