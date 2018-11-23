import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partitioner.ProvincePartitioner;

public class FlowMRProvince {

	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "FlowSumMR");
		job.setJarByClass(FlowMRProvince.class);
		
		job.setMapperClass(FlowSumMRMapper.class);
		job.setReducerClass(FlowSumMRReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		
		//重写Partitioner
		job.setPartitionerClass(ProvincePartitioner.class);
		//设置分区
		job.setNumReduceTasks(4);
		
		FileInputFormat.setInputPaths(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/flow/input"));
		FileOutputFormat.setOutputPath(job, new Path("E:/BigData/7_Hadoop/hadoop-mapreduce-day2/data/flow/inputoutput_location"));
		
		boolean isDone = job.waitForCompletion(true);
		System.exit(isDone ? 0 : 1);
	}
	
	public static class FlowSumMRMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		/**
		 * value  =  1363157993044 	18211575961	94-71-AC-CD-E6-18:CMCC-EASY	120.196.100.99	
		 * iface.qiyi.com	视频网站	15	12	1527	2106	200
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			
			String[] split = value.toString().split("\t");
			
			String outkey = split[1];
			
			String outValue = split[8] + "\t" + split[9];
			
			context.write(new Text(outkey), new Text(outValue));
			
		}
	}
	
	public static class FlowSumMRReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			int upFlow = 0;
			int downFlow = 0;
			int sumFlow = 0;
			
			for(Text t : values){
				String[] split = t.toString().split("\t");
				
				int upTempFlow = Integer.parseInt(split[0]);
				int downTempFlow = Integer.parseInt(split[1]);
				
				upFlow+=upTempFlow;
				downFlow +=  downTempFlow;
			}
			
			sumFlow = upFlow + downFlow;
			
			context.write(key, new Text(upFlow + "\t" + downFlow + "\t" + sumFlow));
		}
	}
}

