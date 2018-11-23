import java.io.IOException;
import java.io.UnsupportedEncodingException;

import javax.swing.JOptionPane;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
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
 * create time：2018年3月15日
 *
 * content：计算学生考试平均成绩
 * 	
 * 源数据路径：E:\BigData\7_Hadoop\hadoop-mapreduce-day2\data\input\score
 * 
 */
public class AvgScore {

	public static void main(String[] args) throws Exception {
		
		Configuration conf=new Configuration();
		
		Job job=Job.getInstance(conf);
		
		job.setJarByClass(AvgScore.class);
		job.setMapperClass(AvgScoreMapper.class);
		job.setReducerClass(AvgScoreReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//输入输出路径
		Path inputPath = new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/score/input/");
		FileInputFormat.setInputPaths(job, inputPath);
		Path outputPath = new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/score/output");
		FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outputPath)){
			fs.delete(outputPath,true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		boolean isDone=job.waitForCompletion(true);
		System.exit(isDone?0:1);
	}
	
	public static class AvgScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		/*
		 * 数据格式：
		 * 	张三 98
		 * 	李四 96
		 * 	王五 95
		 */
		
		Text outKey=new Text();
		Text outValue=new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			transformTextToUTF8(value, "GBK");
			
			String[] split = value.toString().split(" ");
			outKey.set(split[0]);
			outValue.set(split[1]);
			context.write(outKey, new IntWritable(Integer.parseInt(outValue.toString())));	
		}
	}
	
	public static class AvgScoreReducer extends Reducer<Text, IntWritable,Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			transformTextToUTF8(key, "GBK");
			int sum=0;
			int count=0;
			int avg=0;
			for(IntWritable value:values){
				sum+=value.get();
				count++;
			}
			
			avg=sum/count;
			context.write(key,new IntWritable(avg));
		}
	}
	/*
	 * 字符编码转换
	 */
	public static Text transformTextToUTF8(Text text, String encoding) {
		String value = null;
		
		try {
			value = new String(text.getBytes(), 0, text.getLength(), encoding);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		
		return new Text(value);
	}
	
}
