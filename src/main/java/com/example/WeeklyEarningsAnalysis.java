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
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

public class WeeklyEarningsAnalysis {

    // Mapper 类
    public static class EarningsMapper extends Mapper<Object, Text, Text, Text> {
        private Text weekday = new Text();
        private Text earningsValues = new Text();
        private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
        private final SimpleDateFormat dayFormat = new SimpleDateFormat("EEEE", Locale.ENGLISH); // 将日期转换为星期几

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头
            if (key.toString().equals("0") || value.toString().startsWith("日期")) return;

            String[] fields = value.toString().split(",");
            if (fields.length < 3) return;  // 确保数据完整性

            try {
                Date date = dateFormat.parse(fields[0].trim()); // 解析日期
                String dayOfWeek = dayFormat.format(date); // 获取对应的星期几
                String perTenThousandEarnings = fields[1].trim();
                String annualizedYield = fields[2].trim();

                weekday.set(dayOfWeek);
                earningsValues.set(perTenThousandEarnings + "," + annualizedYield); // 设置<万份收益, 七日年化收益>
                context.write(weekday, earningsValues);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    // Reducer 类
    public static class EarningsReducer extends Reducer<Text, Text, Text, Text> {
        private final DecimalFormat earningsFormat = new DecimalFormat("#.0000"); // 保留4位小数
        private final DecimalFormat yieldFormat = new DecimalFormat("#.000");     // 保留3位小数
        private TreeMap<Double, String> sortedEarnings = new TreeMap<>((a, b) -> Double.compare(b, a)); // 按万份收益降序排序

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalEarnings = 0.0;
            double totalYield = 0.0;
            int count = 0;

            for (Text value : values) {
                String[] earningsData = value.toString().split(",");
                if (earningsData.length == 2) {
                    totalEarnings += Double.parseDouble(earningsData[0]);
                    totalYield += Double.parseDouble(earningsData[1]);
                    count++;
                }
            }

            if (count > 0) {
                double avgEarnings = totalEarnings / count;
                double avgYield = totalYield / count;
                String formattedEarnings = earningsFormat.format(avgEarnings);
                String formattedYield = yieldFormat.format(avgYield);
                sortedEarnings.put(avgEarnings, key.toString() + "\t" + formattedEarnings + "," + formattedYield);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<Double, String> entry : sortedEarnings.entrySet()) {
                context.write(new Text(entry.getValue()), null);
            }
        }
    }

    // Driver 类
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Weekly Earnings Analysis");
        job.setJarByClass(WeeklyEarningsAnalysis.class);
        job.setMapperClass(EarningsMapper.class);
        job.setReducerClass(EarningsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
