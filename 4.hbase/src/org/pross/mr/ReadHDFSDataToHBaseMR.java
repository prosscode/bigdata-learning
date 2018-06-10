package org.pross.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 需求：读取HDFS上的数据。插入到HBase库中
 * 
 * hbase.zookeeper.quorum == hadoop02:2181
 */
public class ReadHDFSDataToHBaseMR extends Configured implements Tool{

	@Override
	public int run(String[] arg0) throws Exception {
		
//		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://myha01/");
//		conf.addResource("config/core-site.xml");
//		conf.addResource("config/hdfs-site.xml");
		
		// config === HBaseConfiguration
		Configuration config = HBaseConfiguration.create();
		config.set("hbase.zookeeper.quorum", "hadoop02:2181,hadoop03:2181");
		config.set("fs.defaultFS", "hdfs://myha01/");
		config.addResource("config/core-site.xml");
		config.addResource("config/hdfs-site.xml");
		
		
		System.setProperty("HADOOP_USER_NAME", "hadoop");
		Job job = Job.getInstance(config, "ReadHDFSDataToHBaseMR");
		job.setJarByClass(ReadHDFSDataToHBaseMR.class);
		
		job.setMapperClass(HBaseMR_Mapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		
		// 设置数据读取组件
		job.setInputFormatClass(TextInputFormat.class);
		// 设置数据的输出组件
//		job.setOutputFormatClass(cls);
//		TableMapReduceUtil.initTableReducerJob("student", HBaseMR_Reducer.class, job);
		TableMapReduceUtil.initTableReducerJob("student", HBaseMR_Reducer.class, job, null, null, null, null, false);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);
		
//		FileInputFormat.addInputPath(job, new Path("E:\\bigdata\\hbase\\student\\input"));
		FileInputFormat.addInputPath(job, new Path("/student/input/"));
		
		boolean isDone = job.waitForCompletion(true);
		
		return isDone ? 0: 1;
	}

	public static void main(String[] args) throws Exception {
		
		int run = ToolRunner.run(new ReadHDFSDataToHBaseMR(), args);
		
		System.exit(run);
	}

	
	
	public static class HBaseMR_Mapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		
		/**
		 * 每次读取一行数据
		 * 
		 * Put  ： 构造一个put对象的时候，需要
		 * put 'stduent','95001','cf:name','liyong'
		 * 
		 * 
		 * name:huangbo
		 * age:18
		 * 
		 * name:xuzheng
		 * 
		 */
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {

			context.write(value, NullWritable.get());
			
		}
	}
	
	
	public static class HBaseMR_Reducer extends TableReducer<Text, NullWritable, NullWritable>{
		
		/**
		 * key  ===  95011,包小柏,男,18,MA
		 * 
		 * 95001:  rowkey
		 * 包小柏 : name
		 * 18 : age
		 * 男  ： sex
		 * MA : department
		 * 
		 * column family :  cf
		 */
		@Override
		protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			
			String[] split = key.toString().split(",");
			
			Put put = new Put(split[0].getBytes());
			
			put.addColumn("info".getBytes(), "name".getBytes(), split[1].getBytes());
			put.addColumn("info".getBytes(), "sex".getBytes(), split[2].getBytes());
			put.addColumn("info".getBytes(), "age".getBytes(), split[3].getBytes());
			put.addColumn("info".getBytes(), "department".getBytes(), split[4].getBytes());
			
			context.write(NullWritable.get(), put);
		}
	}
}
