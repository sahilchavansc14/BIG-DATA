import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.StringTokenizer;
import java.io.IOException;

 public class WordCount {
// Mapper class -> output -> string ,int
 public static class WordMapper extends Mapper<Object,Text,Text,IntWritable> {
 Text word = new Text();  //output key value
 public void map(Object key,Text value,Context context) throws IOException,InterruptedException
 {
 StringTokenizer s =new StringTokenizer(value.toString());
 while(s.hasMoreTokens()){
 String token =s.nextToken();
 word.set(token);
 context.write(word, new IntWritable(1));
 }
 }
 }
 
//reducer class -> string,int
public static class WordReducer extends Reducer<Text,IntWritable,Text,IntWritable>{ 
public void reduce(Text key,Iterable<IntWritable>values,Context context) throws IOException,InterruptedException {
IntWritable addition =new IntWritable ();
int sum=0;
for(IntWritable num: values){
sum=sum +num.get();
}
addition.set(sum);
context.write(key,addition);
}
}
public static void main(String args[]) throws Exception
{
 //create the object of configuration class
 Configuration conf = new Configuration();
 
 // create the object of job class
 Job job=new Job(conf,"WordCount");
 
 //set the data type of output key
 job.setOutputKeyClass(Text.class);
 
 //set the data type of output value 
 job.setOutputValueClass(IntWritable.class);
 
 //set the data format of output
 job.setOutputFormatClass(TextOutputFormat.class);
 
 //set the data format of input
 job.setInputFormatClass(TextInputFormat.class);
 
 //set the name of mapper class
 job.setMapperClass(WordMapper.class);
 
 //set the name of reducer class
 job.setReducerClass(WordReducer.class);
 
 // set the input files path from 0 th arguement
 FileInputFormat.addInputPath(job, new Path(args[0]));
 
 // set the output files path from 1 st arguement
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
 //execute the job and wait for completion
 job.waitForCompletion(true);
 }
 }
 
