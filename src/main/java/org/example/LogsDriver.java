package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogsDriver {
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // Set up Hadoop configuration
        Configuration conf = new Configuration();
        // Set the target ip in the job configuration
        conf.set("targetIp", args[2]);
        Job job = Job.getInstance(conf, "Total and Success Requests");
        // specify driver, mapper, reducer classes
        job.setJarByClass(LogsDriver.class);
        job.setMapperClass(LogsMapper.class);
        job.setReducerClass(LogsReducer.class);
        // specify key-value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // Input Output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
