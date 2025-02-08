import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TaskH {
    public static class FriendsMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text outKey = new Text();
        private final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 2) {
                try {
                    outKey.set(fields[2].trim());
                    context.write(outKey, ONE);
                } catch (Exception e) {
                }
            }
        }
    }

    public static class PagesMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final Text outKey = new Text();
        private final IntWritable ZERO = new IntWritable(0);

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length > 0) {
                try {
                    String pageID = fields[0].trim();
                    outKey.set("#" + pageID);
                    context.write(outKey, ZERO);
                } catch (Exception e) {
                }
            }
        }
    }

    public static class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final HashMap<String, Integer> countMap = new HashMap<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String keyStr = key.toString();
            if (keyStr.startsWith("#")) {
                String actualID = keyStr.substring(1);
                if (!countMap.containsKey(actualID)) {
                    countMap.put(actualID, 0);
                }
            } else {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                int newCount = countMap.getOrDefault(keyStr, 0) + sum;
                countMap.put(keyStr, newCount);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int totalCount = 0;
            for (Integer val : countMap.values()) {
                totalCount += val;
            }
            int numIds = countMap.size();
            if (numIds == 0) {
                return;
            }
            int average = totalCount / numIds;
            IntWritable outVal = new IntWritable();
            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                int count = entry.getValue();
                if (count > average) {
                    outVal.set(count);
                    context.write(new Text(entry.getKey()), outVal);
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "TaskH - Count Above Average");
        job.setJarByClass(TaskH.class);
        MultipleInputs.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/friends.csv"), TextInputFormat.class, FriendsMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/pages.csv"), TextInputFormat.class, PagesMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileOutputFormat.setOutputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputH"));
        job.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
