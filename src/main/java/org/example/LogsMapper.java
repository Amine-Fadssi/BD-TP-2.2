package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class LogsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private String targetIp;
    @Override
    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        // Get the ip from the job configuration
        targetIp = context.getConfiguration().get("targetIp");
    }
    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
        String[] logs = value.toString().split(" ");
        String ip = logs[0];
        String code = logs[7];
        context.write(new Text("total"), new IntWritable(1));
        if(ip.equals(targetIp) && code.equals("200"))
            context.write(new Text(targetIp), new IntWritable(1));
    }
}
