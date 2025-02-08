import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskC_Optimized {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private Text nationality = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");

            if (fields.length < 3) {
                return;
            }
            try {
                nationality.set(fields[2].trim());
                context.write(nationality, one);
            } catch (Exception e) {}
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality");
        job.setJarByClass(TaskC_Optimized.class);
        job.setMapperClass(TaskC_Optimized.TokenizerMapper.class);
        // add combiner
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(TaskC_Optimized.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/pages.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputC_Optimized"));
        job.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
