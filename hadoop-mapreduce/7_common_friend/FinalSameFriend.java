import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
 *         create time：2018年3月14日
 *
 *         content：寻找相同的朋友
 * 
 *         数据路径：E:\BigData\7_Hadoop\hadoop-mapreduce-day2\data\friend\input
 * 
 */
public class FinalSameFriend {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(FinalSameFriend.class);

		job.setMapperClass(FinalSameFriendMapper.class);
		job.setReducerClass(FinalSameFriendReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.setInputPaths(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/friend/output"));
		FileOutputFormat.setOutputPath(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/friend/output_2"));

		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);

	}

	/*
	 * 数据格式：
	 * A	F-I-O-K-G-D-C-H-B-
	 * 		表示A是F和I的共同好友，A是F和O的共同好友，A是F和K的共同好友
	 * B	E-J-F-A-
	 * C	B-E-K-A-H-G-F-
	 * D	H-C-G-F-E-A-K-L-
	 * 
	 */
	public static class FinalSameFriendMapper extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] split = value.toString().split("\t");
			String friend = split[0];
			String[] persons = split[1].split("-");

			Arrays.sort(persons);
			for (int i = 0; i < persons.length - 1; i++) {
				for (int j = i + 1; j < persons.length; j++) {
					context.write(new Text(persons[i] + "-" + persons[j]), new Text(friend));
				}
			}
		}
	}

	public static class FinalSameFriendReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			StringBuffer sb = new StringBuffer();
			for (Text t : values) {
				sb.append(t).append(" ");
			}
			context.write(key, new Text(sb.toString()));

		}
	}

}
