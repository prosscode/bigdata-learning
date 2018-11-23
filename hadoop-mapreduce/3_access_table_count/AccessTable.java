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
import org.pross.array.ArraySort;
import org.pross.array.ArraySort.ArraySortMapper;
import org.pross.array.ArraySort.ArraySortReducer;

/**
 * @author pross shawn
 *
 * create time：2018年3月14日
 *
 * content：
 * 		要求编写MapReduce程序算出高峰时间段（如上午10点）哪张表被访问的最频繁，
 * 		以及这段时间访问这张表最多的用户，
 * 		以及这个用户的总时间开销。
 * 
 * 数据表路径：E:\BigData\7_Hadoop\hadoop-mapreduce-day2\data\table\input
 * 结果路径：E:\BigData\7_Hadoop\hadoop-mapreduce-day2\data\table
 * 
 * 表数据格式：TableName(表名)，Time(时间)，User(用户)，TimeSpan(时间开销)
 * 		user12	3:00	u4	0.9
 * 		user13	4:00	u2	9.1
 * 		user14	6:00	u1	6.1
 * 		user15	5:00	u5	5.1
 */
public class AccessTable {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(AccessTable.class);
		
		job.setMapperClass(AccessTableMapper.class);
		job.setReducerClass(AccessTableReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/table/input/tableInfo.txt"));
		FileOutputFormat.setOutputPath(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/table/output"));
		
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);

	}
	
	/*
	 * mapper组件
	 */
	public static class AccessTableMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			String outKey=split[0];
			context.write(new Text(outKey), new IntWritable(1));
		}
	}
	
	/*
	 * reducer组件
	 */
	public static class AccessTableReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int number=0;
			for(IntWritable value:values){
				Text outKey=key;
				number++;
				context.write(outKey, new IntWritable(number));
			}
			
		}
	}
}
