package cn.itcast.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import  java.lang.Long;
import java.io.IOException;


public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {
//    将新的k2、v2 转为k3、v3
//    将k3、v3 写入上下文中

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
//  K2 V2
        /*
        hello <1,1,1,>
        world <1,1>
        hadoop <1>
        ----------------
        K3 V3
        hello 3
        world 2
        hadoop 1
         */
        Long count = 0l;
//        遍历集合，将集合中的数字相加，得到V3
        for (LongWritable value:values){
            count += value.get();
        }
//        将k3和v3写入上下文中
        context.write(key,new LongWritable(count));
    }
}

