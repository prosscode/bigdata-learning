import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

/**
 * @author pross shawn
 *
 * create time：2018年3月8日
 *
 * content：对于操作HDFS的基本工具的集中地
 */
public class HDFSUtils {
	
	public static FileSystem fs=null;
//	测试
	public static void main(String[] args) throws Exception {
//		HDFSUtils.initFileSystem();
//		fs.copyFromLocalFile(new Path("E:/hello.txt"),new Path("/"));
//		HDFSUtils.closeFileSystem();
	}
	
	/**
	 * 初始化FileSystem对象
	 * @throws Exception 
	 */
	public static void initFileSystem() throws Exception{
		Configuration conf=new Configuration();
		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		fs=FileSystem.get(conf);
	}
	
	/**
	 * 关闭FileSystem的连接
	 * @throws Exception
	 */
	public static void closeFileSystem() throws Exception{
		fs.close();
	}
	
	
	/**
	 * 创建一个 固定长度为length最大值为 maxValeu的int类型的数组
	 */
	public static int[] createIntArray(int length, int maxValue) {
		Random r=new Random();
		// 初始化一个数组
		int[] intArray = new int[length];

		// 给初始化好的数组进行随机赋值
		for (int i = 0; i < length; i++) {
			int nextInt = r.nextInt(maxValue);
			intArray[i] = nextInt;
		}

		// 最终的返回结果
		return intArray;
	}
}
