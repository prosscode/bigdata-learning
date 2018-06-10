package org.pross.mr;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 	get 'stduent','rk01'   ====  Result
 * 
 *  需求：读出所有的记录（Result），然后提取出对应的 age 信息
 *  
 *  mapper阶段的  
 *  
 *  	输入： 从hbase来
 *  
 *  		key :  rowkey     
 *      	value :  result
 *      
 *      	ImmutableBytesWritable, Result
 *  
 *      输出： hdfs
 *      
 *      	key :  age
 *          value :    年龄值
 *          
 *  reducer阶段：
 *  
 *  	输入：
 *  
 *      	key :  "age"
 *          value :    年龄值 = 18
 *          
 *     输出：
 *     
 *     		key： NullWritbale
 *     		value: 平均
 */
public class ReadDataFromHBaseToHDFSMR extends Configured implements Tool {

	public static void main(String[] args) throws Exception {

		int run = ToolRunner.run(new ReadDataFromHBaseToHDFSMR(), args);

		System.exit(run);
	}

	@Override
	public int run(String[] arg0) throws Exception {

		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop02:2181,hadoop03:2181");
		config.set("fs.defaultFS", "hdfs://myha01/");
		config.addResource("config/core-site.xml");
		config.addResource("config/hdfs-site.xml");
		System.setProperty("HADOOP_USER_NAME", "hadoop");

		Job job = Job.getInstance(config, "ReadDataFromHBaseToHDFSMR");
		job.setJarByClass(ReadDataFromHBaseToHDFSMR.class);

		
		
		// 从此开始，就是设置当前这个MR程序的各种job细节
		Scan scan  = new Scan();
		scan.addColumn("info".getBytes(), "age".getBytes());
		TableMapReduceUtil.initTableMapperJob(
				"student".getBytes(),	// 指定表名
				scan,   	// 指定扫描数据的条件
				ReadDataFromHBaseToHDFSMR_Mapper.class, 	// 指定mapper class
				Text.class,    	// outputKeyClass mapper阶段的输出的key的类型
				IntWritable.class, // outputValueClass mapper阶段的输出的value的类型
				job,
				false);		// job对象
		
		
		job.setReducerClass(ReadDataFromHBaseToHDFSMR_Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);
		
		
		/**
		 * 在当前的MR程序中。   输入的数据是来自于 HBase，  按照常理来说，需要自定义一个数据读取组件   读 hbase
		 * 
		 * 但是：TableMapReduceUtil.initTableMapperJob 这个方法已经做了。！！！！！！
		 */
		
		FileOutputFormat.setOutputPath(job, new Path("/student/avgage_output2"));
		

		boolean isDone = job.waitForCompletion(true);
		return isDone ? 0 : 1;
	}
	
	
	public static class ReadDataFromHBaseToHDFSMR_Mapper extends TableMapper<Text, IntWritable>{
		
		Text outKey = new Text("age");
		
		/**
		 * key = 就是rowkey
		 * 
		 * value = 就是一个result对象
		 */
		@Override
		protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
			
			boolean containsColumn = value.containsColumn("info".getBytes(), "age".getBytes());
			
			if(containsColumn){
				
				List<Cell> cells = value.getColumnCells("info".getBytes(), "age".getBytes());
				
				Cell cell = cells.get(0);
				
				byte[] cloneValue = CellUtil.cloneValue(cell);
				String age = Bytes.toString(cloneValue);
				
				context.write(outKey, new IntWritable(Integer.parseInt(age)));
			}
		}
	}
	
	public static class ReadDataFromHBaseToHDFSMR_Reducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			
			
			int count = 0;
			int sum = 0;
			
			for(IntWritable iw : values){
				
				count++;
				sum += iw.get();
			}
			
			double  avgAge = sum * 1D / count;
			
			context.write(key, new DoubleWritable(avgAge));
		}
	}
}
