import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author pross shawn
 *
 * create time：2018年3月14日
 *
 * content：数字排序并加序号
 * 	
 * 数据路径：E:\BigData\7_Hadoop\hadoop-mapreduce-day2\data\array\input
 * 结果输出路径：E:\BigData\7_Hadoop\hadoop-mapreduce-day2\data\array\output
 */
public class ArraySort {
	static int  number=0;
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(ArraySort.class);
		
		job.setMapperClass(ArraySortMapper.class);
		job.setReducerClass(ArraySortReducer.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/array/input"));
		FileOutputFormat.setOutputPath(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/array/output2"));
		
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);

	}
	/*
	 * Mapper组件
	 */
	public static class ArraySortMapper extends Mapper<LongWritable,Text,IntWritable,IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String tempNumber = value.toString().trim();
			int outKey=Integer.parseInt(tempNumber);
			context.write(new IntWritable(outKey),new IntWritable(0));
			
		}
	}
	
	/*
	 * Ruduce组件
	 */
	public static class ArraySortReducer extends Reducer<IntWritable,IntWritable, IntWritable, IntWritable>{
		
		@Override
		protected void reduce(IntWritable key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
			for(IntWritable t:values){
				number++;
				context.write(new IntWritable(number),key);
			}
		}
	}
}
