package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class WeeklyFlowAnalysis {

    // Mapper 类
    public static class WeeklyFlowMapper extends Mapper<Object, Text, Text, Text> {
        private Text weekday = new Text();
        private Text flowValues = new Text();
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
        private final SimpleDateFormat dayFormat = new SimpleDateFormat("EEEE", Locale.ENGLISH); // 将日期转换为星期几

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\\s+");
            if (fields.length != 2) return;  // 确保格式正确

            try {
                Date date = dateFormat.parse(fields[0]); // 解析日期
                String dayOfWeek = dayFormat.format(date); // 获取对应的星期几
                String[] flowData = fields[1].split(",");

                if (flowData.length == 2) {
                    weekday.set(dayOfWeek);
                    flowValues.set(flowData[0] + "," + flowData[1]); // 设置<流入量, 流出量>
                    context.write(weekday, flowValues);
                }
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    // Reducer 类
    public static class WeeklyFlowReducer extends Reducer<Text, Text, Text, Text> {
        private TreeMap<Double, String> sortedFlows = new TreeMap<>((a, b) -> Double.compare(b, a)); // 按流入量降序排序

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalInflow = 0.0;
            double totalOutflow = 0.0;
            int count = 0;

            for (Text value : values) {
                String[] flowData = value.toString().split(",");
                if (flowData.length == 2) {
                    totalInflow += Double.parseDouble(flowData[0]);
                    totalOutflow += Double.parseDouble(flowData[1]);
                    count++;
                }
            }

            if (count > 0) {
                double avgInflow = totalInflow / count;
                double avgOutflow = totalOutflow / count;
                sortedFlows.put(avgInflow, key.toString() + "\t" + avgInflow + "," + avgOutflow); // 按平均流入量排序
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Double, String> entry : sortedFlows.entrySet()) {
                context.write(new Text(entry.getValue()), null);
            }
        }
    }

    // Driver 类
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weekly Flow Analysis");
        job.setJarByClass(WeeklyFlowAnalysis.class);
        job.setMapperClass(WeeklyFlowMapper.class);
        job.setReducerClass(WeeklyFlowReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
