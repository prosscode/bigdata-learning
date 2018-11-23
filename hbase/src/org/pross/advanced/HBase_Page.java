package org.pross.advanced;

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.pross.util.HBasePrintUtil;
import org.pross.util.HBaseUtil;

public class HBase_Page {
	
	public static void main(String[] args) throws Exception {
		
		
//		ResultScanner pageData = getPageData(3, 10);
//		ResultScanner pageData = getPageData(2, 2);
		
		
//		ResultScanner pageData1 = getPageData(1, 3);
//		ResultScanner pageData1 = getPageData("baiyc_20150716_0002", 3);
		
		
//		ResultScanner pageData2 = getPageData("rk01", 3);
		ResultScanner pageData2 = getPageData(3, 2000);
		
		HBasePrintUtil.printResultScanner(pageData2);
	}

	/**
	 * @param pageIndex 	第几页
	 * @param pageSize		每一页的记录总数
	 * @return
	 * 
	 * 负责编写JS代码的前端人员。他们是不知道。怎么传入startRow 
	 */
	public static ResultScanner getPageData(int pageIndex, int pageSize) throws Exception{
		
		if(pageSize < 3 || pageSize > 15){
			pageSize = 5;
		}
		
		/**
		 * 当前这个代码的真实作用就是把：；
		 * 
		 * "baiyc_20150716_0001", 3
		 * 
		 * 转换成：
		 * 
		 * 2, 3
		 * 
		 * 难点： 就是  pageIndex 转成 startRow
		 */
		String startRow = getCurrentPageStartRow(pageIndex, pageSize);
		
		return getPageData(startRow, pageSize);
		
	}
	
	
	/**
	 * 当前这个方法的作用：
	 * 
	 * 	就是把   前端人员 穿送过来的    pageIndex  转换成  startRow
	 * 
	 *  以方便调用底层最简单的获取一页分页数据的 方法： getPageData(startRow, pageSize)
	 * 
	 * @param pageIndex
	 * @param pageSize
	 * @return
	 */
	private static String getCurrentPageStartRow(int pageIndex, int pageSize) throws Exception {
		
		// 怎么实现？
		
		
		// 如果 传送过来的额  pageIndex 不合法。 默认返回 第一页数据
		if(pageIndex <= 1){
			
			/*pageIndex == -1
					转成了
			startRow == null*/
			
			return null;
		
		}else{
			
			// 从第二页开始的所有数据。
			String startRow = null;
			
			
			for(int i = 1; i <= pageIndex - 1;  i++){
				
				// 第几次循环，就是获取第几页的数据
				ResultScanner pageData = getPageData(startRow, pageSize);
				
				// 获取当前这一页的最后rowkey
				Iterator<Result> iterator = pageData.iterator();
				Result result = null;
				while(iterator.hasNext()){
					result = iterator.next();
				}
				
				// 让最后一个rowkey往后挪动一点位置，但是又不会等于下一页的 startRow
				String endRowStr = new String(result.getRow());
				byte[] add = Bytes.add(endRowStr.getBytes(), new byte[]{ 0x00});
				String nextPageStartRowStr = Bytes.toString(add);
				
				// 
				startRow = nextPageStartRowStr;
			}
			
			return startRow;
		}
		
	}

	/**
	 * 描述：
	 * 
	 * 		从  startRow开始 查询 pageSize 条数据
	 * 
	 * @param startRow
	 * @param pageSize
	 * @return
	 */
	public static ResultScanner getPageData(String startRow, int pageSize) throws IOException{
		
		
		Connection con = HBaseUtil.getConnection();
//		Admin admin = HBaseUtil.getAdmin();
		
		Table table = HBaseUtil.getTable("user_info");
		
		
		Scan scan = new Scan();
		
		// 设置起始行健搞定
		
		// 如果是第一页数据， 所以  scan.setStartRow这句代码根本就没有任何意义。。 不用设置即可
		if(!StringUtils.isBlank(startRow)){
			
			// 如果用户不传入 startRow, 或者传入了一个 非法的 startRow， 还是按照规则  返回   第一页数据
			scan.setStartRow(startRow.getBytes());
		}
		
		
		// 设置总数据条件
		Filter pageFilter = new PageFilter(pageSize);
		
		scan.setFilter(pageFilter);
		
		ResultScanner scanner = table.getScanner(scan);
		
		
		return scanner;
	}
}
