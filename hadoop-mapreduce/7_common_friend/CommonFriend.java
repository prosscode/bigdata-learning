import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author pross shawn
 *
 *         create time：2018年3月20日
 *
 *         content：找出共同好友
 */
public class CommonFriend{

	public static void main(String[] args) throws Exception {
		Configuration conf1 = new Configuration();
		Configuration conf2 = new Configuration();
		FileSystem fs = FileSystem.get(conf1);

		// 第一个job任务
		Job job1 = Job.getInstance(conf1);
		job1.setJarByClass(CommonFriend.class);
		job1.setMapperClass(CommonFriend1Mapper.class);
		job1.setReducerClass(CommonFriend1Reduce.class);

		job1.setMapOutputKeyClass(Text.class);
		job1.setMapOutputValueClass(Text.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		Path inputPath1 = new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/friend/input");
		Path outputPath1 = new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/friend/output_merge");
		FileInputFormat.setInputPaths(job1, inputPath1);
		if (fs.exists(outputPath1)) {
			fs.delete(outputPath1, true);
		}
		FileOutputFormat.setOutputPath(job1, outputPath1);
		
		//第二个job任务
		Job job2 =Job.getInstance(conf2);
		job2.setMapperClass(CommonFriend2Mapper.class);
		job2.setReducerClass(CommonFriend2Reducer.class);
		job2.setMapOutputKeyClass(Text.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		Path inputPath2 = new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/friend/output_merge");
		Path outputPath2 = new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/friend/output_lastMerge");
		FileInputFormat.setInputPaths(job2, inputPath2);
		if(fs.exists(outputPath2)){
			fs.delete(outputPath2, true);
		}
		FileOutputFormat.setOutputPath(job2, outputPath2);
		
		/*
		 * 多个job串联
		 * 使用JobControl把具有依赖的多个job串联起来
		 */
		JobControl control=new JobControl("CommonFriend");
		ControlledJob ajob=new ControlledJob(job1.getConfiguration());
		ControlledJob bjob=new ControlledJob(job2.getConfiguration());
		// bjob的执行依赖ajob
		bjob.addDependingJob(ajob);
		
		control.addJob(ajob);
		control.addJob(bjob);
		
		Thread t=new Thread(control);
		t.start();
		
		//等待0.5秒执行下一个
		while(!control.allFinished()){
			Thread.sleep(500);
		}
		
		System.exit(0);
	}

	/*
	 * 第一个MR程序
	 */
	public static class CommonFriend1Mapper extends Mapper<LongWritable, Text, Text, Text> {
		/*
		 * 数据格式 A:B,C,D,F,E,O B:A,C,E,K
		 */
		Text outkey = new Text();
		Text outValue = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// 拆分
			String[] user_friends = value.toString().split(":");
			String user = user_friends[0];
			String[] friends = user_friends[1].split(",");
			for (String friend : friends) {
				outkey.set(friend);
				outValue.set(user);
				context.write(outkey, outValue);
			}
		}
	}
	public static class CommonFriend1Reduce extends Reducer<Text, Text, Text, Text> {
		Text keyOut = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<String> userList = new ArrayList<String>();
			for (Text t : values) {
				userList.add(t.toString());
			}
			Collections.sort(userList);
			int size = userList.size();
			for (int i = 0; i < size - 1; i++) {
				for (int j = i + 1; j < size; j++) {
					String outKey = userList.get(i) + "-" + userList.get(j);
					keyOut.set(outKey);
					context.write(keyOut, key);
				}
			}
		}
	}

	
	/*
	 * 第二个MR程序
	 * 数据格式：
	 * B-C	A
	 * B-D	A
	 * B-F	A
	 * B-G	A
	 */
	public static class CommonFriend2Mapper extends Mapper<LongWritable, Text, Text, Text> {
		Text keyOut = new Text();
		Text valueOut = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			keyOut.set(split[0]);
			valueOut.set(split[1]);
			context.write(keyOut, valueOut);
		}
	}

	public static class CommonFriend2Reducer extends Reducer<Text, Text, Text, Text> {
		Text valueOut = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuilder sb = new StringBuilder();
			for (Text t : values) {
				sb.append(t.toString()).append(" ");
			}
			String outValueStr = sb.toString();
			valueOut.set(outValueStr);
			context.write(key, valueOut);
		}
		/*
		 * 输出数据：
		 * A-B	E C 
		 * A-C	D F 
		 */
	}
}
