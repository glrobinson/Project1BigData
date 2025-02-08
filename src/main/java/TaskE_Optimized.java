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
import java.util.HashSet;

public class TaskE_Optimized {

    public static class AccessMapper extends Mapper<Object, Text, IntWritable, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split(",");

            // Get user IDs mapped to the pages they accessed
            Integer userID = Integer.parseInt(fields[1].trim());
            String pageID = fields[2].trim();

            // write the key value pairs
            context.write(new IntWritable(userID), new Text(pageID));
        }
    }

    public static class AccessReducer extends Reducer<IntWritable, Text, IntWritable, Text>{

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            // Int to count total pages visited
            int totalPages = 0;
            // Using a hashset to auto eliminate duplicates
            HashSet<String> distinctPages = new HashSet<>();

            for (Text v : values){
                totalPages++;
                // Add to hash set, if already added, it will not be added again
                distinctPages.add(v.toString());
            }

            // Create result string which includes <total pages visited>,<total unique pages visited>
            String result = totalPages + "," + distinctPages.size();

            context.write(key, new Text(result));
        }
    }
    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "favorites");
        job.setJarByClass(TaskE_Optimized.class);
        job.setMapperClass(TaskE_Optimized.AccessMapper.class);
        job.setReducerClass(TaskE_Optimized.AccessReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/access_logs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputE_Optimized"));
        job.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}