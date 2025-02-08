import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class TaskA {

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {

        private final static String Nationality = "Dominica";

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            String name = fields[1].trim();
            String nationality = fields[2].trim();
            String hobby = fields[4].trim();
            if (nationality.equals(Nationality)) {
                context.write(new Text(name), new Text(hobby));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "nationality");
        job.setJarByClass(TaskA.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/pages.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputA"));
        job.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
