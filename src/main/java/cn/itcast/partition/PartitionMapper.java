package cn.itcast.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PartitionMapper extends Mapper<LongWritable, Text,Text, NullWritable> {
    @Override
// k1 行偏移量， v1一行的文本数据   k2一行的文本数据 v2：nullwritable
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        定义计数器
        final Counter counter = context.getCounter("MR_COUNTER", "partition_counter");
        counter.increment(1l);

        context.write(value,NullWritable.get());
    }
}
