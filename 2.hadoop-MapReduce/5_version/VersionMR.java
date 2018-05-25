import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author pross shawn
 *
 * create time：2018年3月19日
 *
 * content：在所有有版本变动的记录后面追加一条字段信息：该信息就是上一个版本的版本号，只限同用户
 * 
 * 		文 件 路 径：E:\BigData\7_Hadoop\hadoop-mapreduce-day5\data\input_version
 */
public class VersionMR extends Configured implements Tool{


	public static void main(String[] args) throws Exception {
		int run=ToolRunner.run(new VersionMR(), args);
		System.exit(run);
	}

	@Override
	public int run(String[] args) throws Exception {
		//指定HDFS相关参数
		Configuration conf=new Configuration();
		FileSystem fs=FileSystem.get(conf);
		 
		Job job=Job.getInstance(conf);
		job.setJarByClass(VersionMR.class);
		
		//指定mapper类和reduce类
		job.setMapperClass(VersionMRMapper.class);
		job.setReducerClass(VersionMRReduce.class);
		
		//指定输出类型
        job.setMapOutputKeyClass(Version.class);  
        job.setMapOutputValueClass(NullWritable.class);  
        job.setOutputKeyClass(Text.class);  
        job.setOutputValueClass(NullWritable.class); 
        
        //指定路径
        Path inputPath = new Path("E:\\BigData\\7_Hadoop\\hadoop-mapreduce-day5\\data\\input_version");  
        Path outputPath = new Path("E:\\BigData\\7_Hadoop\\hadoop-mapreduce-day5\\data\\output_version"); 
        //如果输出路径存在则删除
       if(fs.exists(outputPath)){
    	   fs.delete(outputPath,true);
       }
       FileInputFormat.setInputPaths(job, inputPath);
       FileOutputFormat.setOutputPath(job, outputPath);
       
       //提交任务
       boolean bool = job.waitForCompletion(true);
       return bool? 0 : 1 ;
	}
	
	
	/**
	 * map组件
	 */
	public static class VersionMRMapper extends Mapper<LongWritable, Text, Version, NullWritable>{
		/*
		 * 数据格式：
		 * 20170308,黄渤,光环斗地主,8,360手机助手,0.1版本,北京
		 * 20170308,黄渤,光环斗地主,5,360手机助手,0.1版本,北京
		 * 
		 * 字段信息：
		 * 用户ID，用户名，游戏名，小时，数据来源，游戏版本，用户所在地
		 * id, name, game, hour, source, version, city 
		 * 
		 */
		@Override
		protected void map(LongWritable key, Text value, Context context) 
				throws IOException ,InterruptedException {
			//拆分数据
			String[] split = value.toString().split(",");
			//把数据封装到对象中
			Version version = new Version(split[0], split[1], split[2], Integer.parseInt(split[3]), split[4], split[5], split[6]);
			context.write(version, NullWritable.get());
		}
		
	}

	/**
	 * reduce组件
	 */
	public static class VersionMRReduce extends Reducer<Version, NullWritable, Text, NullWritable>{
		
		String lastID=null;
		String lastName=null;
		String lastVersion=null;
		
		@Override
		protected void reduce(Version key, Iterable<NullWritable> values,Context context)
				throws IOException, InterruptedException {
			for(NullWritable nvl:values){
				if(lastVersion==null){
					//第一次进入程序，lastVersion为空，直接打印，因为相当于没有上一条数据
					context.write(new Text(key.toString()), NullWritable.get());
				}else{
					//当ID和Name一致时
					if(lastID.equals(key.getId()) && lastName.equals(key.getName())){
						//判断上个版本号和当前版本号是否一致
						if(!lastVersion.equals(key.getVersion())){
							context.write(new Text(key.toString()+"-"+lastVersion), NullWritable.get());
						}
						//当ID和Name有一个不一致时，证明是两个不同的用户
					}else{
						context.write(new Text(key.toString()), NullWritable.get());
					}
				}
				//进行数据迭代处理，方便本次和下次的数据进行对比
				lastID=key.getId();
				lastName=key.getName();
				lastVersion=key.getVersion();
			}
		}
	}

}
