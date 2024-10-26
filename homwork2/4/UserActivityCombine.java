import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class UserActivityCombine {

    public static class ConstellationMap {
        private static final Map<String, Integer> constellationMap = new HashMap<>();

        static {
            // 初始化星座映射，假设星座名称是固定的，并且按照月份排序
            constellationMap.put("白羊座", 1);
            constellationMap.put("金牛座", 2);
            constellationMap.put("双子座", 3);
            constellationMap.put("巨蟹座", 4);
            constellationMap.put("狮子座", 5);
            constellationMap.put("处女座", 6);
            constellationMap.put("天秤座", 7);
            constellationMap.put("天蝎座", 8);
            constellationMap.put("射手座", 9);
            constellationMap.put("摩羯座", 10);
            constellationMap.put("水瓶座", 11);
            constellationMap.put("双鱼座", 12);
        }

        public static int getConstellationIndex(String constellation) {
            return constellationMap.getOrDefault(constellation, -1);
        }
    }

    public static class UserActivityMapper extends Mapper<LongWritable, Text, Text, Text> {
        private boolean isSecondFile = false;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            String inputFilePath = context.getConfiguration().get("mapreduce.input.fileinputformat.inputdir");
            isSecondFile = inputFilePath.contains("second");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts;
            if (isSecondFile) {
                // 第二个文件的格式：user_id sex city constellation
                parts = line.split(","); // 使用空格分割
            } else {
                // 第一个文件的格式：user_id,activedays
                parts = line.split("\t"); // 使用逗号分割
            }

            if (parts.length > 1) {
                String userId = parts[0]; // 用户ID
                context.write(new Text(userId), new Text(line));
            }
        }
    }

    public static class UserActivityReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();
            for (Text val : values) {
                sb.append(val.toString()).append("\n");
            }
            String combined = sb.toString();
            String[] parts = combined.split("\n");
            String output = null;

            for (String line : parts) {
                String[] lineValues = line.split(",");
                if (lineValues.length > 3) {
                    int constellationIndex = ConstellationMap.getConstellationIndex(lineValues[3]);
                    output = line.replace(lineValues[3], Integer.toString(constellationIndex));
                    break; // 假设第一个匹配的行包含星座信息
                }
            }

            if (output != null) {
                context.write(key, new Text(output));
            } else {
                context.getCounter("UserActivityCombine", "Missing Constellation").increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 3) {
            System.err.println("Usage: UserActivityCombine <in> <in2> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "user activity");
        job.setJarByClass(UserActivityCombine.class);
        job.setMapperClass(UserActivityMapper.class);
        job.setReducerClass(UserActivityReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0])); // 第一个文件路径
        FileInputFormat.addInputPath(job, new Path(otherArgs[1])); // 第二个文件路径

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}