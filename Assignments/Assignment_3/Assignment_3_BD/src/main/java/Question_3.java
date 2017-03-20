import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question_3 {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(new Configuration());
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length !=3 ) {
			System.err.println("Usage: Qestion_3 <path_to_business.csv> <path_to_review.csv> <output path>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Question_3");
		job.setJarByClass(Question_3.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, Map_B.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class, Map_R.class );
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// true stands for recursively deleting the folder you gave
		fs.delete(new Path(otherArgs[2]), true);
		
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);		
	}
	
	public static class Map_B extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			if(businessData[1].toLowerCase().contains("Stanford".toLowerCase()))
				context.write(new Text(businessData[0]),new Text("business^"));			
		}
	}
	
	public static class Map_R extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			context.write(new Text(businessData[2]),new Text("review^"+businessData[1]+"^"+businessData[3]));
		}
	}
	
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private HashMap<String,String> review_HashMap = new HashMap<String,String>();
		private HashMap<String,String> business_HashMap = new HashMap<String,String>();

		@Override
		public void reduce(Text businessID, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			for (Text val : values) {
				String delims = "^";
				String[] value = StringUtils.split(val.toString(),delims);
				if(value[0].compareTo("business")==0){
					business_HashMap.put(businessID.toString(), " ");
				}
				if(value[0].compareTo("review")==0){
					review_HashMap.put(businessID.toString(), value[1] + "^" + value[2]);
				}
			}
		}		
		
		@Override
		protected void cleanup(Context context) throws IOException,	InterruptedException {
			for(String businessID : review_HashMap.keySet()) {
				if(business_HashMap.containsKey(businessID)){
					String userIDAndRating=review_HashMap.get(businessID);
					String delims = "^";
					String[] data = StringUtils.split(userIDAndRating.toString(),delims);
					context.write(new Text(data[0]),new Text(data[1]));
				}
			}
		}
	}
}

