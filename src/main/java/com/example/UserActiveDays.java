package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeMap;

public class UserActiveDays {

    // Mapper 类
    public static class ActiveDaysMapper extends Mapper<Object, Text, Text, Text> {
        private Text userId = new Text();
        private Text date = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 跳过表头
            if (key.toString().equals("0")) return;

            String[] fields = value.toString().split(",");
            if (fields.length < 9) return;  // 确保数据完整性

            String id = fields[0].trim();
            double directPurchaseAmt = Double.parseDouble(fields[5].trim());  // 第6列为 direct_purchase_amt
            double totalRedeemAmt = Double.parseDouble(fields[8].trim());  // 第9列为 total_redeem_amt
            String activityDate = fields[1].trim(); // 假设第2列为日期

            if (directPurchaseAmt > 0 || totalRedeemAmt > 0) {
                userId.set(id);
                date.set(activityDate);
                context.write(userId, date); // 输出<用户ID, 日期>
            }
        }
    }

    // Reducer 类
    public static class ActiveDaysReducer extends Reducer<Text, Text, Text, IntWritable> {
        private TreeMap<Integer, String> sortedUsers = new TreeMap<>((a, b) -> b - a); // 按活跃天数降序排序

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueDates = new HashSet<>();  // 用于统计活跃天数

            for (Text date : values) {
                uniqueDates.add(date.toString());  // 添加唯一的活跃日期
            }

            int activeDays = uniqueDates.size();  // 活跃天数
            sortedUsers.put(activeDays, key.toString() + "\t" + activeDays); // 活跃天数作为排序键
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (String entry : sortedUsers.values()) {
                context.write(new Text(entry), null);
            }
        }
    }

    // Driver 类
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "User Active Days");
        job.setJarByClass(UserActiveDays.class);
        job.setMapperClass(ActiveDaysMapper.class);
        job.setReducerClass(ActiveDaysReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0])); // 输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 输出路径

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

