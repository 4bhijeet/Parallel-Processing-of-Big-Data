/*Project 2 Stage 1: Team  - Abhijeet Deshpade (ad79), Mayur Tale Deshmukh (mayurvin)*/

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;

public class ClassCount {
	public static class TokenizerMapper
	extends Mapper<Object, Text, Text, IntWritable>{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		String unknown = "unknown";
		String arr = "arr";
		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			List<String> tokenList = Arrays.asList(value.toString().split(","));
			String hallName = null;
			if(tokenList.get(2).indexOf(" ") != -1){
				
				hallName = tokenList.get(2).substring(0, tokenList.get(2).indexOf(" "));
			}
			else{
				hallName = tokenList.get(2);
			}
			if(!unknown.equalsIgnoreCase(hallName.toLowerCase()) && !arr.equalsIgnoreCase(hallName.toLowerCase())){
				if(isInteger(tokenList.get(7).toString())){
					if(Integer.parseInt(tokenList.get(7).toString()) >= 0){ 
						String genKey =  hallName + "_" + tokenList.get(1);				
						word.set(genKey);
						context.write(word, new IntWritable(Integer.parseInt(tokenList.get(7).toString())));			
					}
				}
			}
			
		}
	}
	public static class IntSumReducer
	extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		public void reduce(Text key, Iterable<IntWritable> values,
				Context context
				) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : values) {
				System.out.println(val.get());
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static boolean isInteger( String input )
	{
	   try
	   {
      		Integer.parseInt( input );
		return true;
	   }
	   catch( Exception e )
	   {
	      return false;
	   }
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "class count");
		job.setJarByClass(ClassCount.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
