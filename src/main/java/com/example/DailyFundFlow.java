package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DailyFundFlow {

    public static class FundFlowMapper extends Mapper<Object, Text, Text, Text> {
        private Text date = new Text();
        private Text amounts = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",");
            try {
                // 提取日期、资金流入和流出量，分别在第2、5、9列
                String reportDate = columns[1].trim();
                double totalPurchaseAmt = columns[4].isEmpty() ? 0.0 : Double.parseDouble(columns[4].trim());
                double totalRedeemAmt = columns[8].isEmpty() ? 0.0 : Double.parseDouble(columns[8].trim());

                // 设置输出的key和value
                date.set(reportDate);
                amounts.set(totalPurchaseAmt + "," + totalRedeemAmt);
                context.write(date, amounts);
            } catch (Exception e) {
                // 忽略格式错误的行
            }
        }
    }

    public static class FundFlowReducer extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalInflow = 0.0;
            double totalOutflow = 0.0;

            for (Text val : values) {
                String[] amounts = val.toString().split(",");
                totalInflow += Double.parseDouble(amounts[0]);
                totalOutflow += Double.parseDouble(amounts[1]);
            }

            // 格式化输出：日期 资金流入量,资金流出量
            result.set(totalInflow + "," + totalOutflow);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Daily Fund Flow");
        job.setJarByClass(DailyFundFlow.class);

        job.setMapperClass(FundFlowMapper.class);
        job.setReducerClass(FundFlowReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
