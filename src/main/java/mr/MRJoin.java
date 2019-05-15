package mr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @ClassName: MRJoin
 * @Auther: zhoucc
 * @Date: 2019/5/15 11:37
 * @Description: join 连接
 */
public class MRJoin {
    public static class MR_Join_Mapper extends Mapper<LongWritable, Text, TextPair, Text> {

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // 获取输入文件的全路径和名称
            String pathName = ((FileSplit) context.getInputSplit()).getPath().toString();
            if (pathName.contains("data.txt")) {
                String values[] = value.toString().split("\t");
                if (values.length < 3) {
                    // data 数据格式不正确，字段小于3，抛弃
                    return;
                } else {
                    TextPair tp = new TextPair(new Text(values[1]), new Text("1"));
                    context.write(tp, new Text(values[0] + "\t" + values[2]));
                }
            }

            if (pathName.contains("info.txt")) {
                String values[] = value.toString().split("\t");
                if (values.length < 2) {
                    // info 数据格式不正确，字段小于2，抛弃
                    return;
                } else {
                    TextPair tp = new TextPair(new Text(values[0]), new Text("0"));
                    context.write(tp, new Text(values[1]));
                }
            }
        }
    }

    public static class MR_Join_Reduce extends Reducer<TextPair, Text, Text, Text> {
        @Override
        protected void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Text pid = key.getFirst();
            String desc = values.iterator().next().toString();
            while (values.iterator().hasNext()) {
                context.write(pid, new Text(values.iterator().next().toString() + "\t" + desc));
            }
        }
    }

    public static class MR_Join_Partitioner extends Partitioner<TextPair, Text> {

        @Override
        public int getPartition(TextPair textPair, Text text, int i) {
            return Math.abs(textPair.getFirst().hashCode()*127) % i;
        }
    }

    public static class MR_Join_Comparator extends WritableComparator {
        public MR_Join_Comparator(){
            super(TextPair.class, true);
        }

        @Override
        public int compare(WritableComparable a, WritableComparable b){
            TextPair ip1 = (TextPair) a;
            TextPair ip2 = (TextPair) b;
            return ip1.getFirst().compareTo(ip2.getFirst());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        if (args.length < 3) {
            System.err.println("Usage:MRJoin <in_path_one> <in_path_two> <output>");
            System.exit(2);
        }
        Job job = new Job(conf, "MRJoin");
        job.setJarByClass(MRJoin.class);
        job.setMapperClass(MR_Join_Mapper.class);
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(Text.class);
        // 设置partition
        job.setPartitionerClass(MR_Join_Partitioner.class);
        job.setGroupingComparatorClass(MR_Join_Comparator.class);
        job.setReducerClass(MR_Join_Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //设置输入和输出的目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
