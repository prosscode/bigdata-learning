/**
 * 
 */
package org.pross.friend;

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
import org.pross.table.AccessTable;
import org.pross.table.AccessTable.AccessTableMapper;
import org.pross.table.AccessTable.AccessTableReducer;

/**
 * @author pross shawn
 *
 * create time：2018年3月14日
 *
 * content：寻找相同的朋友
 * 		
 * 	数据路径：E:\BigData\7_Hadoop\hadoop-mapreduce-day2\data\friend\input
 *  
 */
public class SameFriend {
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(SameFriend.class);
		
		job.setMapperClass(SameFriendMapper.class);
		job.setReducerClass(SameFriendReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/friend/input"));
		FileOutputFormat.setOutputPath(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/friend/output"));
		
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);

	}
	
	/*
	 * 数据格式：
	 * 
	 * A:B,C,D,F,E,O
	 * B:A,C,E,K
	 * C:F,A,D,I
	 */
	public static class SameFriendMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] person = value.toString().split(":");
			String[] friends = person[1].split(",");
			
			String outValue = person[0];
			for(String outKey:friends){
				context.write(new Text(outKey), new Text(outValue));
			}
		}
	}
	
	public static class SameFriendReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb=new StringBuffer();
			for(Text t:values){
				sb.append(t).append("-");
			}
			context.write(key, new Text(sb.toString()));

		}
	}
	
	
}
