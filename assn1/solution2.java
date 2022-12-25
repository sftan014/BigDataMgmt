// A1 Task 2
// 15/10/2022
// Display car number & speed if it's above 70km/h & save to /user/output directory

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

public class solution2 {

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "average");
    job.setJarByClass(solution2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntMinMaxReducer.class);
    job.setReducerClass(IntMinMaxReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private Text carNum = new Text(); // 1st data car num
    private Text date = new Text();   // 2nd data date 
    private final static IntWritable speed = new IntWritable(0);  // 3rd data speed 

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        carNum.set(itr.nextToken());
        date.set(itr.nextToken());
        speed.set( Integer.parseInt(itr.nextToken()) );

        context.write(carNum, speed);
      }
    }
  }

    public static class IntMinMaxReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable>values, Context context) throws IOException,InterruptedException {
            int avg = 0; 
            int sum = 0; 
            int counter = 0;

            for (IntWritable val : values) {
                sum = sum + val.get(); // .get() returns int value
                counter++; // count num of occurances
            }
            avg = sum / counter;

            // set speed if abv 70km/h
            if (avg > 70) {
                result.set(avg);
                context.write(key, result); // display results
            }
        }
    }
}