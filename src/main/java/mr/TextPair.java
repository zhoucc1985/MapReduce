package mr;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @ClassName: TextPair
 * @Auther: zhoucc
 * @Date: 2019/5/15 11:29
 * @Description: 自定义文本pair类
 */
public class TextPair implements WritableComparable<TextPair> {
    private Text first;
    private Text second;

    public void set(Text first, Text second) {
        this.first = first;
        this.second = second;
    }

    public TextPair(){
        set(new Text(), new Text());
    }

    public TextPair(Text first, Text second) {
        set(first, second);
    }

    @Override
    public int compareTo(TextPair o) {
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

    public Text getFirst() {
        return first;
    }

    public void setFirst(Text first) {
        this.first = first;
    }

    public Text getSecond() {
        return second;
    }

    public void setSecond(Text second) {
        this.second = second;
    }

}
