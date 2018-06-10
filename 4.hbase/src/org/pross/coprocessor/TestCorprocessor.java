package org.pross.coprocessor;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

/**
 * 
 * @author pross shawn
 *
 * create time：2018年4月3日
 *
 * content：协处理器
 */
public class TestCorprocessor extends BaseRegionObserver{
	private static final String ZK_CONNECT_KEY = "hbase.zookeeper.quorum";
	private static final String ZK_CONNECT_VALUE = "hadoop02:2181,hadoop03:2181,hadoop04:2181,hadoop05:2181";
	private static Connection con = null;
	private static Table table = null;
	private static Configuration conf = null;

	static{
		conf=HBaseConfiguration.create();
		conf.set(ZK_CONNECT_KEY, ZK_CONNECT_VALUE);
		try {
			con = ConnectionFactory.createConnection(conf);
			table=con.getTable(TableName.valueOf("guanzhu"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability)
			throws IOException {
		byte[] row = put.getRow();
		Cell cell = put.get("cf".getBytes(), "from".getBytes()).get(0);
		Put putIndex= new Put(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
		//交换rowkey和value的值,放到guanzhu表中
		putIndex.addColumn("cf".getBytes(),"from".getBytes(), row);
		table.put(putIndex);
		table.close();
	}
}
