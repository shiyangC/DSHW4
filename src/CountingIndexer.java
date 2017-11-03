import java.io.IOException;
//import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class CountingIndexer {

  public static class TokenizerMapper 
       extends Mapper<Object, Text, Text, Text>{
    
    private Text word = new Text();
    private Text chap = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      FileSplit fileSplit = (FileSplit)context.getInputSplit();
      String filename = fileSplit.getPath().getName();
      chap.set(filename);
      StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("\\W", " "));
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, chap);
      }
    }
  }
  
  public static class IntSumReducer 
       extends Reducer<Text, Text, Text, Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      HashMap<String, Integer> chapSum = new HashMap<String, Integer>();
      String word = key.toString();

      for (Text val : values) {
        String chap = val.toString();
        if (chapSum.containsKey(chap)) {
          int sum = chapSum.get(chap);
          sum++;
          chapSum.put(chap, sum);
        } else {
          chapSum.put(chap, 1);
        }
      }
      String res="";
      for (Map.Entry<String, Integer> pair : chapSum.entrySet()) {
        res=res+pair.getKey()+":"+pair.getValue()+"\n";
      }
      result.set(res);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: wordcount <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "word count");
    job.setJarByClass(CountingIndexer.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    for (int i = 0; i < otherArgs.length - 1; ++i) {
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    }
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}