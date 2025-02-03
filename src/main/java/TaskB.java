import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.Comparator;
import java.util.Map;
import java.util.PriorityQueue;

public class TaskB {
    // this is the function to count page accesses
        public static class TokenizerMapper
                extends Mapper<Object, Text, IntWritable, IntWritable> {

            private IntWritable pID = new IntWritable();
            private final static IntWritable one = new IntWritable(1);

            public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {
                String[] fields = value.toString().split(",");
                if (fields.length < 4) {
                    return;
                }

                try {
                    int p = Integer.parseInt(fields[2].trim());
                    pID.set(p);
                    context.write(pID, one);
                } catch (NumberFormatException e) {}
            }
        }

    public static class IntSumReducer
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class TokenizerMapper2 extends Mapper<Object, Text, IntWritable, IntWritable> {
            private IntWritable pID = new IntWritable();
            private IntWritable count = new IntWritable();

            public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
                String[] fields = value.toString().split("\t");
                if (fields.length < 2) {
                    return;
                }
                pID.set(Integer.parseInt(fields[0]));
                count.set(Integer.parseInt(fields[1]));
                context.write(pID, count);
            }
    }

    // this is the function that extracts the top 10 pages
    public static class IntSumReducer2
            extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {
        private PriorityQueue<Map.Entry<Integer, Integer>> top = new PriorityQueue<>(
                10, Comparator.comparingInt(Map.Entry::getValue)
        );

        public void reduce(IntWritable key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            top.add(new AbstractMap.SimpleEntry<>(key.get(), sum));
            if (top.size() > 10) {
                top.poll();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (!top.isEmpty()) {
                Map.Entry<Integer, Integer> entry = top.poll();
                context.write(new IntWritable(entry.getKey()), new IntWritable(entry.getValue()));
            }
        }
    }

    public static class TokenizerMapper3 extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable pID = new IntWritable();
        private Text info = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 5) {
                return;
            }
            try {
                pID.set(Integer.parseInt(fields[0]));
                info.set(fields[1] + "," + fields[2]);
                context.write(pID, info);
            } catch (NumberFormatException e) {}
        }
    }

    public static class TokenizerMapper4 extends Mapper<Object, Text, IntWritable, Text> {
        private IntWritable pID = new IntWritable();
        private Text count = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) {
                return;
            }
            pID.set(Integer.parseInt(fields[0]));
            count.set(fields[1]);
            context.write(pID, count);
        }
    }

    public static class IntSumReducer3
            extends Reducer<IntWritable,Text,IntWritable,Text> {
        private String info = null;
        private String count = null;

        public void reduce(IntWritable key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            info = null;
            count = null;
            for (Text val : values) {
                String fields = val.toString();
                if (fields.contains(",")) {
                    info = fields;
                } else {
                    count = fields;
                }
            }
            if (info != null && count != null) {
                context.write(key, new Text(info));
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);

        Job job = Job.getInstance(conf, "count access");
        job.setJarByClass(TaskB.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/access_logs.csv"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputBCount"));
        job.waitForCompletion(true);


        Job job2 = Job.getInstance(conf, "top 10 pages");
        job2.setJarByClass(TaskB.class);
        job2.setMapperClass(TokenizerMapper2.class);
        job2.setReducerClass(IntSumReducer2.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputBCount"));
        FileOutputFormat.setOutputPath(job2, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputBTop"));
        job2.waitForCompletion(true);

        Job job3 = Job.getInstance(conf, "results");
        job3.setJarByClass(TaskB.class);
        MultipleInputs.addInputPath(job3, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputBTop"),
                TextInputFormat.class, TokenizerMapper4.class);
        MultipleInputs.addInputPath(job3, new Path("/Users/gracerobinson/Project1_BigData/Project1/input/pages.csv"),
                TextInputFormat.class, TokenizerMapper3.class);
        job3.setReducerClass(IntSumReducer3.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job3, new Path("/Users/gracerobinson/Project1_BigData/Project1/output/outputB"));
        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
