import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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

public class Question_4 {
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length !=3 ) {
			System.err.println("Usage: Question_4 <path_to_review.csv> <path_to_user.csv> <output path>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "Question_4");
		job.setJarByClass(Question_4.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class ,Map_R.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class,Map_U.class );
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class Map_R extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			context.write(new Text(businessData[1]),new Text("review"));
		}
	}
	public static class Map_U extends Mapper<LongWritable, Text, Text, Text>	{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			context.write(new Text(businessData[0]),new Text("user^" + businessData[1] ));
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private HashMap<String,Integer> review_HashMap = new HashMap<String,Integer>();
		private HashMap<String,String> user_HashMap = new HashMap<String,String>();

		@Override
		public void reduce(Text userID, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			int sum=0;
			for (Text val : values) {
				String delims = "^";
				String[] value = StringUtils.split(val.toString(),delims);
				if(value[0].compareTo("user")==0){
					user_HashMap.put(userID.toString(), value[1]);
				}
				if(value[0].compareTo("review")==0){
					sum++;
				}
			}
			review_HashMap.put(userID.toString(), sum);
		}		
		
		@Override
		protected void cleanup(Context context) throws IOException,	InterruptedException {
			Compare_Map c =  new Compare_Map(review_HashMap);
	        int count=0;
	        TreeMap<String,Integer> Result_TreeMap = new TreeMap<String,Integer>(c);
	        Result_TreeMap.putAll(review_HashMap);
			for(String userID : Result_TreeMap.keySet()) {
				if(user_HashMap.containsKey(userID)){
					count++;
					if(count>10)
						break;
					String userName=user_HashMap.get(userID);
					context.write(new Text(userID),new Text(userName + "\t" + review_HashMap.get(userID)));
				}
			}
		}
	}
	public static class Compare_Map implements Comparator<String> {

		HashMap<String, Integer> base;

		public Compare_Map(HashMap<String,Integer> base) {
			this.base = base;
		}

		public int compare(String a, String b) {
			if (base.get(a) >= base.get(b)) {
				return -1;
			} else {
				return 1;
			}
		}
	}
	
	

}

