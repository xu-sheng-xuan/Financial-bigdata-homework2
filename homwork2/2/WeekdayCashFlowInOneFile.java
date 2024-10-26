import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Iterator;

public class WeekdayCashFlowInOneFile {

    public static class WeekdayCashFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) {
            try {
                String line = value.toString();
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    String dateStr = parts[0];
                    SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
                    Date date = sdf.parse(dateStr);
                    SimpleDateFormat weekdayFormat = new SimpleDateFormat("EEEE");
                    String weekday = weekdayFormat.format(date);
                    String[] amounts = parts[1].split(",");
                    context.write(new Text(weekday), new Text(amounts[0] + "," + amounts[1]));
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class WeekdayCashFlowReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalPurchase = 0;
            long totalRedeem = 0;
            int count = 0;
            List<Long> purchases = new ArrayList<>();
            List<Long> redeems = new ArrayList<>();
            for (Text value : values) {
                String[] amounts = value.toString().split(",");
                long purchase = Long.parseLong(amounts[0]);
                long redeem = Long.parseLong(amounts[1]);
                totalPurchase += purchase;
                totalRedeem += redeem;
                purchases.add(purchase);
                redeems.add(redeem);
                count++;
            }
            long avgPurchase = totalPurchase / count;
            long avgRedeem = totalRedeem / count;
            context.write(key, new Text(avgPurchase + "," + avgRedeem));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weekday Cash Flow Statistics");
        job.setJarByClass(WeekdayCashFlowInOneFile.class);
        job.setMapperClass(WeekdayCashFlowMapper.class);
        job.setReducerClass(WeekdayCashFlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}