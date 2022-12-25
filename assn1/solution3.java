// A1 Task 3
// 15/10/2022
// Find the min, max, avg, sum of the data of SpeedCamera.txt

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

public class solution3 {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count"); 
    job.setJarByClass(solution3.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntMinMaxReducer.class); // prevent from performing 2 reducer
    job.setReducerClass(IntMinMaxReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    // main waits until all jobs are completed before stopping
    System.exit(job.waitForCompletion(true) ? 0 : 1); 
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable counter = new IntWritable(0);
    private Text word = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        counter.set( Integer.parseInt(itr.nextToken()) );
        context.write(word, counter); 
      }
    }
  }

  public static class IntMinMaxReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, Context context
                      ) throws IOException, InterruptedException {
      int max = Integer.MIN_VALUE;
      int min = Integer.MAX_VALUE; 
      int avg = 0; 
      int sum = 0; 
      int counter = 0;

      for (IntWritable val : values) {
        if (val.get()> max )
          max = val.get();

        if (val.get() < min )
          min = val.get();
        
          sum = sum + val.get(); // .get() returns int value
          counter++; // count num of occurances
      }

      avg = sum / counter;

      // Display all the results
      result.set(max);
      context.write(new Text("Maximum " + key), result);

      result.set(min);
      context.write(new Text("Minimum " + key), result);

      result.set(avg);
      context.write(new Text("Average " + key), result);

      result.set(sum);
      context.write(new Text("Total " + key), result);
    }
  }
}
