package third;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UserActivityAnalysis {

    public static class ActivityMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text userId = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (key.get() > 0) {
                String[] parts = line.split(",");
                if (parts.length > 9) {
                    long userIdValue = Long.parseLong(parts[0]);
                    long totalRedeemAmt = Long.parseLong(parts[9]);
                    boolean isActive = totalRedeemAmt > 0;
                    userId.set(String.valueOf(userIdValue));
                    // 输出用户ID和活跃标记
                    context.write(userId, isActive ? one : new IntWritable(0));
                }
            }
        }
    }

    public static class ActivityReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private Map<String, Integer> userActivityMap = new HashMap<>();

        @Override
        protected void reduce(Text userId, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int activeDays = 0;
            for (IntWritable val : values) {
                activeDays += val.get();
            }
            userActivityMap.put(userId.toString(), activeDays);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 将 map 转换为 list，便于排序
            List<Map.Entry<String, Integer>> sortedUserList = new ArrayList<>(userActivityMap.entrySet());

            // 按照活跃天数从大到小排序
            Collections.sort(sortedUserList, new Comparator<Map.Entry<String, Integer>>() {
             
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue().compareTo(o1.getValue());
                }
            });

            // 输出排序后的用户ID及其活跃天数
            int rank = 1;
            for (Map.Entry<String, Integer> entry : sortedUserList) {
                String outputValue = entry.getKey() + "\t" + entry.getValue();
                context.write(new Text(outputValue), null);
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Activity Analysis");
        job.setJarByClass(UserActivityAnalysis.class);
        job.setMapperClass(ActivityMapper.class);
        job.setReducerClass(ActivityReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}