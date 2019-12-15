package cn.itcast.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * k2,text v2,nullwritable
 *
 * k3:text,v3,nullwirtalbe
 */
public class PartitionerReducer extends Reducer <Text, NullWritable,Text,NullWritable>{
    public static enum Counter{
        MY_INPUT_RECORDES,MY_INPUT_BYPES
    }
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
//        方式2：使用枚举定义计数器
        context.getCounter(Counter.MY_INPUT_RECORDES).increment(1l);
        context.write(key,NullWritable.get());
    }
}
