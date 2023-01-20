import java.util.*;
import org.apache.hadoop.conf.*;
import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;



   public class fblive
 {
   public static class FbMapper extends  MapReduceBase implements Mapper<Object,Text,Text,IntWritable>  {
 // map function
 boolean flag=false;
public void map(Object key,Text value,OutputCollector<Text,IntWritable>output,Reporter reporter) throws IOException {
if(flag) {
String line=value.toString();
StringTokenizer s=new StringTokenizer(line,",");
String id=s.nextToken();
String type=s.nextToken();
String date=s.nextToken();
int count=0,likes=0;
while(count < 4){
likes = Integer.parseInt(s.nextToken());
count++;
}
if (date.startsWith("2") && date.contains("2018") && type.equals("video"))
output.collect(new Text("Likes"),new IntWritable (likes));
}
flag =true;
}
}
//reducer class -> string,int
public static class FbReducer extends MapReduceBase implements  Reducer <Text,IntWritable,Text,IntWritable>{ 
//reduce function
public void reduce(Text key,Iterator< IntWritable> values,OutputCollector<Text,IntWritable>output,Reporter reporter) throws IOException {
int add=0;
while(values.hasNext())
   add=add+values.next().get();
output.collect(key, new IntWritable(add));
}
}
public static void main(String args[]) throws Exception
{
 JobConf conf = new JobConf(fblive.class);
 conf.setJobName("Facebook Likes");
 conf.setOutputKeyClass(Text.class);
 conf.setOutputValueClass(IntWritable.class);
 conf.setMapperClass(FbMapper.class);
 conf.setReducerClass(FbReducer.class);
 conf.setInputFormat(TextInputFormat.class);
 conf.setOutputFormat(TextOutputFormat.class);
 FileInputFormat.setInputPaths(conf, new Path(args[0]));
 FileOutputFormat.setOutputPath(conf, new Path(args[1]));
 JobClient.runJob(conf);
 }
 }

 
 
 
