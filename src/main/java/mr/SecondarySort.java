package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * @ClassName: SecondarySort
 * @Auther: zhoucc
 * @Date: 2019/5/14 15:54
 * @Description: 二次排序
 */
public class SecondarySort {

    public static class TheMapper extends Mapper<LongWritable, Text, IntPair, NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] field = value.toString().split("\t");
            int field1 = Integer.parseInt(field[0]);
            int field2 = Integer.parseInt(field[1]);
            context.write(new IntPair(field1, field2), NullWritable.get());
        }

    }

    public static class TheReduce extends Reducer<IntPair, NullWritable, IntPair, NullWritable> {
        protected void reduce(IntPair key, Iterable<IntPair> values, Context context) throws IOException, InterruptedException {
            context.write(key, NullWritable.get());
        }
    }

    public static class FirstPartitioner extends Partitioner<IntPair, NullWritable> {
        @Override
        public int getPartition(IntPair intPair, NullWritable nullWritable, int i) {
            return Math.abs(intPair.getFirst().get()) % i;
        }
    }

    public static class KeyComparator extends WritableComparator {
        public KeyComparator(){
            super(IntPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            IntPair ip1 = (IntPair) a;
            IntPair ip2 = (IntPair) b;
            //第一列按升序排列
            int cmp = ip1.getFirst().compareTo(ip2.getFirst());
            if(cmp != 0) {
                return cmp;
            }
            //在第一列相等的情况下，第二列按倒序排序
            return (-1)*ip1.getSecond().compareTo(ip2.getSecond());
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        // 读取配置文件
        Configuration conf = new Configuration();
        if (args.length < 3) {
            System.err.println("Usage:input,output and reduce numbers");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        // 设置主类
        job.setJarByClass(SecondarySort.class);
        job.setMapperClass(TheMapper.class);
        job.setMapOutputKeyClass(IntPair.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setPartitionerClass(FirstPartitioner.class);
        job.setSortComparatorClass(KeyComparator.class);
        job.setReducerClass(TheReduce.class);
        job.setOutputKeyClass(IntPair.class);
        job.setOutputValueClass(NullWritable.class);
        // 输入路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // 输出路径
        int reduceNum = 1;
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (args.length >= 3 && args[2] != null) {
            reduceNum = Integer.parseInt(args[2]);
        }
        job.setNumReduceTasks(reduceNum);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
