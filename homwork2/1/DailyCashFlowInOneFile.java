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
import java.util.Iterator;

public class DailyCashFlowInOneFile {

    public static class DailyCashFlowMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length > 10) {
                
                if (parts[4].matches("\\d+") && parts[8].matches("\\d+")) {
                    String reportDate = parts[1];
                    long totalPurchaseAmt = Long.parseLong(parts[4]);
                    long totalRedeemAmt = Long.parseLong(parts[8]);
                    context.write(new Text(reportDate), new Text(totalPurchaseAmt + "," + totalRedeemAmt));
                }
            }
        }
    }

    public static class DailyCashFlowReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalPurchase = 0;
            long totalRedeem = 0;
            Iterator<Text> iterator = values.iterator();
            while (iterator.hasNext()) {
                String[] amounts = iterator.next().toString().split(",");
                totalPurchase += Long.parseLong(amounts[0]);
                totalRedeem += Long.parseLong(amounts[1]);
            }
            context.write(key, new Text(totalPurchase + "," + totalRedeem));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Daily Cash Flow Statistics");
        job.setJarByClass(DailyCashFlowInOneFile.class);
        job.setMapperClass(DailyCashFlowMapper.class);
        job.setReducerClass(DailyCashFlowReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)? 0 : 1);
    }
}