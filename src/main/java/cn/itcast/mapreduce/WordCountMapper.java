package cn.itcast.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 4个泛型
 * K1，V1，K2，V2
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

//  map方法就是将K1、V1转为K2、V2
    /*
    k1 v1
    0 hello，world，hadoop
    15  hdfs，hive，hello
    k2 v2
    hello   1
    world   1
    hdfs    1
    hadoop  1
    hello   1
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        将一行的文本数据进行拆分
        Text text = new Text();
        LongWritable longWritable = new LongWritable();
        final String[] split = value.toString().split(",");
        for(String word: split){
            text.set(word);
            longWritable.set(1);
            context.write(text,longWritable);
        }

//        遍历数组，组装k2 和v2

//        将k2和v2 写入上下文中
    }
}
