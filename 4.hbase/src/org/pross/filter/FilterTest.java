package org.pross.filter;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.pross.util.HBasePrintUtil;

/**
 * @author pross shawn
 *
 * create time：2018年3月30日
 *
 * content： 过滤器的分页操作
 */
@SuppressWarnings("deprecation")
public class FilterTest {

	private static final String ZK_CONNECT_STR = "hadoop02:2181,hadoop03:2181,hadoop04:2181";
	private static final String TABLE_NAME = "user_info";

	private static Configuration config = null;
	private static HTable table = null;
	static {
		config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", ZK_CONNECT_STR);
		try {
			table = new HTable(config, TABLE_NAME);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
		
//		ResultScanner pageData = getPageData("zhangsan_20150701_0001", 4);
		//
		ResultScanner pageData = getPageData(1, 4);
		HBasePrintUtil.printResultScanner(pageData);
		
	}
	
	/*
	 * 起始索引和每页显示的数据数目
	 */
	public static ResultScanner getPageData(int pageIndex, int pageNumber) throws Exception{

		String startRow = null;
		
		if(pageIndex == 1){	
			// 当客户方法只取第一页的分页数据时，
			ResultScanner pageData = getPageData(startRow, pageNumber);
			return pageData;
			
		}else{
			ResultScanner newPageData = null;
			for(int i=0; i<pageIndex - 1; i++){	// 总共循环次数是比你取的页数少1
				newPageData = getPageData(startRow, pageNumber);
				startRow = getLastRowkey(newPageData);
				byte[] add = Bytes.add(Bytes.toBytes(startRow), new byte[]{0X00});
				startRow = Bytes.toString(add);
			}
			newPageData = getPageData(startRow, pageNumber);
			return newPageData;
		}
	}
	
	/**
	 * 定义起始row和显示的数据的数目
	 * @param startRow
	 * @param pageNumber
	 * @return
	 * @throws Exception 
	 */
	public static ResultScanner getPageData(String startRow, int pageNumber) throws Exception{
		Scan scan  = new Scan();
		scan.addColumn(Bytes.toBytes("base_info"), Bytes.toBytes("name"));
		// 设置當当前查询的起始位置
		if(!StringUtils.isBlank(startRow)){
			scan.setStartRow(Bytes.toBytes(startRow));
		}
		// 第二个参数
		Filter pageFilter = new PageFilter(pageNumber);
		scan.setFilter(pageFilter);
		
		ResultScanner rs = table.getScanner(scan);
		return rs;
	}
	
	public static String getLastRowkey(ResultScanner rs){
		String lastRowkey = null;
		for(Result result : rs){
//			System.out.println(result.getRow());
			lastRowkey = Bytes.toString(result.getRow());
		}
		return lastRowkey;
	}
}
