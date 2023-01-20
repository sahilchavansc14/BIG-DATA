import java.util.*;
import org.apache.hadoop.conf.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



   public class fblive2
 {
   public static class FbMapper extends  MapReduceBase implements Mapper<Object,Text,IntWritable,IntWritable>  {
 // map function
 boolean flag=false;
public void map(Object key,Text value,OutputCollector<IntWritable,IntWritable>output,Reporter reporter) throws IOException {
String line[]=value.toString().split(",",10);
if(flag) {
String date =line[2];
//date1[]=date.split("/",3);
int share =Integer.parseInt(line[5]);
if (date.contains("2017")){
String x[]=date.split("/",3);
int month=Integer.parseInt(x[0]);
output.collect(new IntWritable(month),new IntWritable(share));
}
}
flag=true;
}
}
//reducer class -> string,int
public static class FbReducer extends MapReduceBase implements  Reducer <IntWritable,IntWritable,IntWritable,FloatWritable>{ 
//reduce function
public void reduce(IntWritable key,Iterator< IntWritable> values,OutputCollector<IntWritable,FloatWritable>output,Reporter reporter) throws IOException {
int sum=0,total=0;
while(values.hasNext()){
   sum=sum+values.next().get();
   total++;
   }
output.collect(key, new FloatWritable(sum/(float)total));
}
}
public static void main(String args[]) throws Exception
{
 JobConf conf = new JobConf(fblive2.class);
 conf.setJobName("Facebook Likes");
 conf.setOutputKeyClass(IntWritable.class);
 conf.setOutputValueClass(FloatWritable.class);
 conf.setMapOutputKeyClass(IntWritable.class);
 conf.setMapOutputValueClass(IntWritable.class);
 conf.setMapperClass(FbMapper.class);
 conf.setReducerClass(FbReducer.class);
 conf.setInputFormat(TextInputFormat.class);
 conf.setOutputFormat(TextOutputFormat.class);
 FileInputFormat.setInputPaths(conf, new Path(args[0]));
 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 JobClient.runJob(conf);
 }
 }

 
 
 
