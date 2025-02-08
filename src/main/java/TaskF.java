import org.apache.commons.lang.StringUtils;
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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TaskF {

    // identify p1 who never accessed their friend's Facebook page p2
    public static class FriendshipMapper extends Mapper<Object, Text, IntWritable, NullWritable> {
        private Map<Integer, Set<Integer>> accessMap = new HashMap<>();
        private final IntWritable outKey = new IntWritable();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new IOException("Oh no something is missing! sad!");
            }

            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] fields = line.split(",");
                if (fields.length < 3) continue;

                try {
                    int personID = Integer.parseInt(fields[1].trim());
                    int pageID = Integer.parseInt(fields[2].trim());

                    accessMap.computeIfAbsent(personID, k -> new HashSet<>()).add(pageID);
                } catch (NumberFormatException ignored) {}
            }
            IOUtils.closeStream(reader);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 3) return;

            try {
                int p1 = Integer.parseInt(fields[1].trim());
                int p2 = Integer.parseInt(fields[2].trim());

                // check if p1 never accessed p2's Facebook page
                Set<Integer> accessedPages = accessMap.getOrDefault(p1, new HashSet<>());
                if (!accessedPages.contains(p2)) {
                    outKey.set(p1);
                    context.write(outKey, NullWritable.get());
                }
            } catch (NumberFormatException ignored) {}
        }
    }

    public static class DeduplicationReducer extends Reducer<IntWritable, NullWritable, IntWritable, NullWritable> {
        @Override
        protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context)
                throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    // get the name of the person
    public static class NameMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private Map<String, String> pagesMap = new HashMap<>();
        private Text outText = new Text();

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
            // header is now not included so disregared
            String line;
            boolean isHeader = true;
            while ((line = reader.readLine()) != null) {
                if (isHeader) {
                    isHeader = false;
                    continue;
                }
                String[] fields = line.split(",");
                if (fields.length < 2) continue;

                pagesMap.put(fields[0].trim(), fields[1].trim());
            }
            IOUtils.closeStream(reader);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String personID = value.toString().trim();
            String name = pagesMap.get(personID);

            if (name != null) {
                outText.set(personID + "," + name);
                context.write(outText, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Job job1 = Job.getInstance(conf, "non accessed page");
        job1.setJarByClass(TaskF.class);
        job1.setMapperClass(FriendshipMapper.class);
        job1.setReducerClass(DeduplicationReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job1, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/friends.csv"));
        FileOutputFormat.setOutputPath(job1, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputFNonAccess"));
        job1.addCacheFile(new Path("/Users/gracerobinson/Project1_BigData/Project1/input/access_logs.csv").toUri());
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance(conf, "final with names");
        job2.setJarByClass(TaskF.class);
        job2.setMapperClass(NameMapper.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputFNonAccess"));
        FileOutputFormat.setOutputPath(job2, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputF"));
        job2.addCacheFile(new Path("/Users/gracerobinson/Project1_BigData/Project1/input/pages.csv").toUri());
        job2.waitForCompletion(true);
        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
