package cn.itcast.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
//泛型是k2、v2的类型
public class MyPartitioner extends Partitioner<Text, NullWritable> {
    @Override
//    定义分区规则
//    返回对应的分区编号
    public int getPartition(Text text, NullWritable nullWritable, int i) {
//       拆分k2
        final String[] split = text.toString().split("\t");
//        判断中奖字段和15的关系，然后返回对应分区编号
        final String numStr = split[5];

        if (Integer.parseInt(numStr) > 15){
            return 1;
        }else {
            return 0;
        }


    }
}
