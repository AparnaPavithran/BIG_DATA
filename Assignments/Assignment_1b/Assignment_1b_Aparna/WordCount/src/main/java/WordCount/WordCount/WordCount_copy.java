package WordCount.WordCount;

import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;


public class WordCount_copy {
	
		  public static class TokenizerMapper
		       extends Mapper<Object, Text, Text, IntWritable>{
			  
				private static  Set<String> stopWordList = new HashSet<String>();
			
				
		    private final static IntWritable one = new IntWritable(1);
		    private Text word = new Text();
		    
		    
		    public void map(Object key, Text value, Context context)
					throws IOException, InterruptedException {
				String line = value.toString();
				StringTokenizer tokenizer = new StringTokenizer(line);

				while (tokenizer.hasMoreTokens()) {
					String token = tokenizer.nextToken();
					if (stopWordList.contains(token)) {
						context.getCounter(COUNTERS.STOPWORDS)
								.increment(1L);
					} else {
						context.getCounter(COUNTERS.GOODWORDS)
								.increment(1L);
						word.set(token);
						context.write(word, one);
					}
				}
			}
		    
		    public void setup(Context context) throws java.io.IOException,
			InterruptedException {

		
		try{
            Path pt=new Path("/user/axp161730/assignment_2/part1/stopwords.txt");//Location of file in HDFS
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            line=br.readLine();
            while (line != null){
                System.out.println(line);
                stopWordList.add(line);
                line=br.readLine();
            }
        }catch(Exception e){
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
		        sum += val.get();
		      }
		      result.set(sum);
		      context.write(key, result);
		    }
		  }

		public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		    conf.set("mapreduce.framework.name", "yarn");
		    Job job = Job.getInstance(conf, "word count");
		    job.setJarByClass(WordCount.class);
		    job.setMapperClass(TokenizerMapper.class);
		    job.setCombinerClass(IntSumReducer.class);
		    job.setReducerClass(IntSumReducer.class);
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(IntWritable.class);
		  
			// Logic to read the location of stop word file from the command line
			// The argument after -skip option will be taken as the location of stop
			// word file
		   
		    
			
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    System.exit(job.waitForCompletion(true) ? 0 : 1);
		  }
		  public enum COUNTERS {
			  STOPWORDS,
			  GOODWORDS
			 }
	

}

