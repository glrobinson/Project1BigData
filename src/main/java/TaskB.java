import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.PriorityQueue;

public class TaskB {

    // Count Accesses
    public static class TokenizerMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private IntWritable pID = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 4) return;

            try {
                int p = Integer.parseInt(fields[2].trim());
                pID.set(p);
                context.write(pID, one);
            } catch (NumberFormatException ignored) {}
        }
    }

    // Extract Top 10 Pages
    public static class PartB_Reducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        public static class PageRankByCount implements Comparable<PageRankByCount> {
            public int pageId, accesses;

            public PageRankByCount(int pageId, int accesses) {
                this.pageId = pageId;
                this.accesses = accesses;
            }

            @Override
            public int compareTo(PageRankByCount obj2) {
                return Integer.compare(this.accesses, obj2.accesses);
            }
        }

        private PriorityQueue<PageRankByCount> topTen = new PriorityQueue<>();
        private IntWritable outKey = new IntWritable();
        private IntWritable outValue = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) sum += val.get();

            topTen.add(new PageRankByCount(key.get(), sum));
            if (topTen.size() > 10) topTen.poll();
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (PageRankByCount page : topTen) {
                outKey.set(page.pageId);
                outValue.set(page.accesses);
                context.write(outKey, outValue);
            }
        }
    }

    // Mapper-Side Join using Distributed Cache
    public static class ReplicatedJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
        private HashMap<String, String> pagesData = new HashMap<>();
        private Text outputText = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles == null || cacheFiles.length == 0) {
                throw new RuntimeException("Pages file is not set in Distributed Cache");
            }

            Path path = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            FSDataInputStream fis = fs.open(path);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fis, StandardCharsets.UTF_8));

            String line;
            while (StringUtils.isNotEmpty(line = reader.readLine())) {
                String[] fields = line.split(",");
                if (fields.length >= 3) {
                    pagesData.put(fields[0], fields[1] + "," + fields[2]);  // Store PageID -> "PageName, Nationality"
                }
            }
            IOUtils.closeStream(reader);
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) return;

            String pageID = fields[0];
            String accessCount = fields[1];

            if (pagesData.containsKey(pageID)) {
                outputText.set(pageID + "\t" + pagesData.get(pageID));
                context.write(outputText, NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws Exception {
        long timeNow = System.currentTimeMillis();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        // Count Accesses and Find Top 10
        Job job1 = Job.getInstance(conf, "Count Page Accesses");
        job1.setJarByClass(TaskB.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(PartB_Reducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/access_logs.csv"));
        Path outputPath1 = new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputBTop");
        FileOutputFormat.setOutputPath(job1, outputPath1);
        job1.waitForCompletion(true);

        // Mapper-Side Join with Pages.csv
        Job job2 = Job.getInstance(conf, "Join Top 10 Pages with Metadata");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(ReplicatedJoinMapper.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        Path outputPath2 = new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputB");
        FileOutputFormat.setOutputPath(job2, outputPath2);
        // Set Pages.csv as Distributed Cache File
        job2.addCacheFile(new URI("/Users/gracerobinson/Project1_BigData/Project1/input/pages.csv"));
        FileInputFormat.addInputPath(job2, outputPath1);  // Read from Top 10 pages
        job2.waitForCompletion(true);

        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) /1000.0;
        System.out.println(seconds + "  seconds");
    }
}
