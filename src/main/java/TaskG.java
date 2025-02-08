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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;

public class TaskG {

    public static class AccessMapper extends Mapper<Object, Text, IntWritable, Text>{
        // The goal of this mapper is to map each user with their access dates
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split(",");

            // Get user IDs mapped to their access dates
            Integer userID = Integer.parseInt(fields[1].trim());
            String date = fields[4].trim();

            // write the key value pairs
            context.write(new IntWritable(userID), new Text(date));
        }
    }

    public static class PagesMapper extends Mapper<Object, Text, IntWritable, Text>{
        // The goal of this mapper is to map each user ID with their name
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            String[] fields = value.toString().split(",");
            Integer personID = Integer.parseInt(fields[0].trim());
            String name = fields[1].trim();

            context.write(new IntWritable(personID), new Text("N-" + name));
        }
    }

    public static class AccessReducer extends Reducer<IntWritable, Text, IntWritable, Text>{
        // The goal of this reducer will take all the access dates for each user, first it will find the latest
        // Access date for that user. Next it will determine if it is 14 days old. If it is, it will return the
        // User as disconnected
        private static final DateTimeFormatter dtformatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
            String name = "";
            // Use the oldest date for comparison to find most recent access date
            Instant latestDate = Instant.EPOCH;
            // Find current date minus 14 days for comparison
            Instant dateminus14days = Instant.now().minus(14, ChronoUnit.DAYS);

            for (Text v : values){
                // Now that we have access times, and names mapped to a user ID
                // We can find the name, and determine if the uer is disconnected in a single reducer
                // This is why we started the name string with "N-" to identify what value it is easily

                if(v.toString().startsWith("N-")){
                    name = v.toString().substring(2);
                }
                else{
                    // find access time and parse it into formatted date
                    LocalDateTime accDate = LocalDateTime.parse(v.toString(), dtformatter);
                    // convert to instant
                    Instant access = accDate.atZone(ZoneId.systemDefault()).toInstant();

                    // Determine the most recent access date
                    if(access.isAfter(latestDate)){
                        latestDate = access;
                    }
                }
            }

            // Now that we have the latest access date stored in latestDate
            // determine if this date makes the user disconnected or active

            if(latestDate.isBefore(dateminus14days) && name != ""){
                context.write(key, new Text(name));
            }
        }
    }
    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "disconnected");
        job.setJarByClass(TaskG.class);
        // Because we need to read multiple files, explain which mapper should look at which file
        MultipleInputs.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/access_logs.csv"),
                TextInputFormat.class, AccessMapper.class);
        MultipleInputs.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/pages.csv"),
                TextInputFormat.class, PagesMapper.class);
        job.setReducerClass(TaskG.AccessReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputG"));
        job.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}