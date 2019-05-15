package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: Counters
 * @Auther: zhoucc
 * @Date: 2019/5/15 10:36
 * @Description: 自定义计数器
 */
public class Counters {

    public static class MyCounterMap extends Mapper<LongWritable, Text, Text, Text> {
        public static Counter ct = null;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String arr_value[] = value.toString().split("\t");
            if (arr_value.length > 3) {
                //ErrorCounter为组名，toolong为组成员
                ct = context.getCounter("ErrorCounter", "toolong");
                ct.increment(1);
            } else if (arr_value.length < 3) {
                ct = context.getCounter("ErrorCounter", "tooshort");
                ct.increment(1);
            } else {

            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        if (args.length != 2) {
            System.err.println("Usage: Counters <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Counter");
        job.setJarByClass(Counters.class);
        job.setMapperClass(MyCounterMap.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
