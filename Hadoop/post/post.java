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



   public class post
 {
   public static class FunnyMapper extends  Mapper<Object,Text,Text,IntWritable>  {
 // map function
 boolean flag=false;
 int max=0;
 String type=null;
public void map(Object key,Text value,Context context) throws IOException,InterruptedException {
String line[]=value.toString().split(",",12);
if(flag) {
if ( Integer.parseInt(line[9]) > max){
max=Integer.parseInt(line[9]);
type=line[1]+"\t"+line[2];
}
}
flag=true;
}
public void cleanup(Context context) throws IOException,InterruptedException {
context.write(new Text(type),new IntWritable(max));
}
}
public static void main(String args[]) throws Exception
{
 //create the object of configuration class
 Configuration conf = new Configuration();
 
 // create the object of job class
 Job job=new Job(conf,"Funniest Post");
 
 //set the data type of output key
 job.setOutputKeyClass(Text.class);
 
 //set the data type of output value 
 job.setOutputValueClass(IntWritable.class);
 
 //set the data format of output
 job.setOutputFormatClass(TextOutputFormat.class);
 
 //set the data format of input
 job.setInputFormatClass(TextInputFormat.class);
 
 //set the name of mapper class
 job.setMapperClass(FunnyMapper.class);
 
 //set the name of reducer class
job.setNumReduceTasks(0); 
 // set the input files path from 0 th arguement
 FileInputFormat.addInputPath(job, new Path(args[0]));
 
 // set the output files path from 1 st arguement
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 
 //execute the job and wait for completion
 job.waitForCompletion(true);
 }
 }
 

 
 
 
