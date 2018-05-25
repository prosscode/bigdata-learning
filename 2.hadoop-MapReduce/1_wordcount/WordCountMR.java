import java.io.IOException;

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
 * 
 * @author pross shawn
 *
 * create time：2018年3月17日
 *
 * content：例子程序 wordcount
 */
public class WordCountMR {

	public static void main(String[] args) throws Exception {
		//指定hdfs相关的参数
		Configuration conf = new Configuration();
//		conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
//		System.setProperty("HADOOP_USER_NAME", "hadoop");
		FileSystem fs = FileSystem.get(conf);
		
		//通过Configuration对象获取job对象，job对象会组织所有的该mapreduce程序所有的各种组件
		Job job = Job.getInstance(conf);
		//设置jar包所在路径，一般即为类名的反射
		job.setJarByClass(WordCountMR.class);
		
		//指定mapper和reducer类
		job.setMapperClass(WordCountMRMapper.class);
		job.setReducerClass(WordCountMRReducer.class);
		//指定mapper和reduce的输入输出类型，如果reducer的输入输出类型和mapper一致可以省略
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//设置程序的输入路径和输出路径
		Path inputPath = new Path("E:\\BigData\\7_Hadoop\\hadoop-mapreduce-day2\\data\\score\\input");
		Path outputPath = new Path("E:\\BigData\\7_Hadoop\\hadoop-mapreduce-day2\\data\\score\\output_wordcount");
		FileInputFormat.setInputPaths(job, inputPath);
		//判断输出路径是否存在，存在则删除
		if(fs.exists(outputPath)){
			fs.delete(outputPath, true);
		}
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//提交任务，布尔值决定要不要将运行进度信息输出给用户
		boolean isDone = job.waitForCompletion(true);
		//主线程根据mapreduce程序的运行结果成功与否决定是否退出
		System.exit(isDone ? 0 : 1);
	}
	
	/**
	 * mapper组件
	 * LongWritable key : 该key就是value该行文本的在文件当中的起始偏移量 
     * Text value ： 就是MapReduce框架默认的数据读取组件TextInputFormat读取文件当中的一行文本 
	 * 
	 */
	public static class WordCountMRMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		//定义输出类型，避免重复实例化其对象
		Text outKey=new Text();
		IntWritable outValue=new IntWritable();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//切分单词
			String[] words=value.toString().split(" ");
			
			for(String word:words){
				//每个单词计数一次，也就是把单词组织成<hello，1>这样的K-V
				outKey.set(word);
				outValue.set(1);
				context.write(outKey, outValue);
			}
		}
	}
	
	/**
	 * reduce组件
	 * 输入类型就是Map阶段的处理结果，输出类型就是Reduce最后的输出 
	 * reducetask将这些收到K-V数据拿来处理时，是这样调用我们的reduce方法的： 
	 * 		先将自己收到的所有的K-V对按照K分组（根据K是否相同） 将某一组K-V中的第一个K-V中的K传给reduce方法的key变量
	 * 		把这一组kv中所有的v用一个迭代器传给reduce方法的变量values 
	 */
	public static class WordCountMRReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			//结果汇总
			int sum=0;
			for(IntWritable v:values){
				sum+=v.get();
			}
			//输出
			context.write(key, new IntWritable(sum));
		}
	}
}
