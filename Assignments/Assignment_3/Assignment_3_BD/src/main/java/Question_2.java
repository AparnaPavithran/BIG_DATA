import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Question_2 {
	
	//private static String inputString;

	//public static String User_Name;
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		conf.set("User_Name", otherArgs[3].trim());
		if (otherArgs.length !=4 ) {
			System.err.println("Usage: Question_2 <path_to_review.csv> <path_to_user.csv> <output path> <NameOfUser>");
			System.exit(2);
		}
		//User_Name=otherArgs[3];
		//conf.set("input1",otherArgs[3]);
		Job job = Job.getInstance(conf, "Question_2");
		job.setJarByClass(Question_2.class);
		MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class ,Map_R.class );
		MultipleInputs.addInputPath(job, new Path(otherArgs[1]),TextInputFormat.class,Map_U.class );
		
		//JobConf job1 = new JobConf();
		//job1.set("inputString", otherArgs[3]);
		
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	public static class Map_R extends Mapper<LongWritable, Text, Text, Text>{
		
		/*public void configure(JobConf job1) {
	    inputString = job1.get("inputString");
		}*/
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			context.write(new Text(businessData[1]),new Text("review^"+businessData[3]));
		}
	}
	
	public static class Map_U extends Mapper<LongWritable, Text, Text, Text>	{
		
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String delims = "^";
			String[] businessData = StringUtils.split(value.toString(),delims);
			//if(User_Name.contentEquals(businessData[1]))
			//if(inputString.contentEquals(businessData[1]))
			/*String test ="Kerri H.";
			if(test.equals(businessData[1]))	{
				context.write(new Text(businessData[0]),new Text("user^" + businessData[1] + "^" + businessData[2]));
			}*/
			//if(test.)
			//if(test.contentEquals(businessData[1]))
			//	context.write(test, businessData[1]);
			if(context.getConfiguration().get("User_Name").contentEquals(businessData[1]))
				context.write(new Text(businessData[0]),new Text("user^" + businessData[1] + "^" + businessData[2]));
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private HashMap<String,Double> review_HashMap = new HashMap<String,Double>();
		private HashMap<String,String> user_HashMap = new HashMap<String,String>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			int count=0;
			int sum=0;
			for (Text val : values) {
				String delims = "^";
				String[] value = StringUtils.split(val.toString(),delims);
				if(value[0].compareTo("user")==0)	{
					user_HashMap.put(key.toString(), value[1] + "^" + value[2]);
				}
				if(value[0].compareTo("review")==0)	{
					sum+=Double.parseDouble(value[1]);
					count++;
					review_HashMap.put(key.toString(), (double)sum/count);
				}
			}
		}		
		
		@Override
		protected void cleanup(Context context) throws IOException,	InterruptedException {
			Compare_Map c =  new Compare_Map(review_HashMap);
	        TreeMap<String,Double> sorted_map = new TreeMap<String,Double>(c);
	        sorted_map.putAll(review_HashMap);
	        int count=0;
			for(String userID : sorted_map.keySet()) {
				if(user_HashMap.containsKey(userID))	{
					String addressAndCategoriesData=user_HashMap.get(userID);
					count++;
					String delims = "^";
					String[] data = StringUtils.split(addressAndCategoriesData.toString(),delims);
					context.write(new Text(userID),new Text(data[0] + "\t" + review_HashMap.get(userID)));
				}
			}
		}
	}
	
	public static class Compare_Map implements Comparator<String> {
		HashMap<String,Double> base;

		public Compare_Map(HashMap<String,Double> base) {
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

