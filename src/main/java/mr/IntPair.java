package mr;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: IntPair
 * @Auther: zhoucc
 * @Date: 2019/5/14 15:53
 * @Description: 自定义整形pair类
 */
public class IntPair implements WritableComparable<IntPair> {

    private IntWritable first;
    private IntWritable second;

    /**
     * 功能描述: 需要加无参构造函数，否则反射时会报错
     *
     * @auther: zhoucc
     * @date: 2019/5/14 17:55
     */
    public IntPair(){
        set(new IntWritable(), new IntWritable());
    }

    public IntPair(int field1, int field2) {
        set(new IntWritable(field1), new IntWritable(field2));
    }

    public void set(IntWritable first, IntWritable second){
        this.first = first;
        this.second = second;
    }

    public void set(int first, int second){
        set(new IntWritable(first), new IntWritable(second));
    }

    public void setFirst(IntWritable first) {
        this.first = first;
    }

    public void setSecond(IntWritable second) {
        this.second = second;
    }

    public IntPair(IntWritable first, IntWritable second) {
        this.first = first;
        this.second = second;
    }

    public IntWritable getFirst() {
        return first;
    }

    public IntWritable getSecond() {
        return second;
    }

    @Override
    public int compareTo(IntPair o) {
        int cmp = first.compareTo(o.first);
        if (cmp != 0) {
            return cmp;
        }
        return second.compareTo(o.second);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        first.write(dataOutput);
        second.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        first.readFields(dataInput);
        second.readFields(dataInput);
    }

    /**
     * 输出格式化
     */
    @Override
    public String toString(){
        return first + "\t" + second;
    }

     @Override
     public int hashCode(){
        return first.hashCode()*163 + second.hashCode();
     }

     @Override
     public boolean equals(Object o) {
        if (o instanceof IntPair) {
            IntPair tp = (IntPair)o;
            return first.equals(tp.first) && second.equals(tp.second);
        }
        return false;
     }
}
