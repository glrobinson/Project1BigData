import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class TaskD {

    // Get the p2 and their friends
    public static class FriendsMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private IntWritable p2 = new IntWritable();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 3) return;

            try {
                int person2 = Integer.parseInt(fields[2].trim());
                p2.set(person2);
                context.write(p2, one);
            } catch (NumberFormatException e) {}
        }
    }

    // Sum the number of times each p2 is listed as a friend
    public static class ConnectednessReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            result.set(count);
            context.write(key, result);
        }
    }

    // Replace p2 with their name
    // mapper join
    public static class OwnerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Map<String, String> ownerMap = new HashMap<>();
        private Map<String, String> friendCountMap = new HashMap<>();
        private Text outputText = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("Pages file is missing");
            }

            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                ownerMap.put(fields[0].trim(), fields[1].trim());
            }
            IOUtils.closeStream(reader);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) return;

            String p2 = fields[0];
            String count = fields[1];
            friendCountMap.put(p2, count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, String> entry : ownerMap.entrySet()) {
                String p2 = entry.getKey();
                String name = entry.getValue();

                String count = friendCountMap.getOrDefault(p2, "0");
                outputText.set(name + "," + count);
                context.write(outputText, NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job1 = Job.getInstance(conf, "connectedness factor");
        job1.setJarByClass(TaskD.class);
        job1.setMapperClass(FriendsMapper.class);
        job1.setReducerClass(ConnectednessReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/friends.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputDConnect"));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "final");
        job2.setJarByClass(TaskD.class);
        job2.setMapperClass(OwnerMapper.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        job2.addCacheFile(new Path("/Users/gracerobinson/Project1_BigData/Project1/input/pages.csv").toUri());
        FileInputFormat.addInputPath(job2, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputDConnect"));
        FileOutputFormat.setOutputPath(job2, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputD"));
        job2.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
