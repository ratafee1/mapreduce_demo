package cn.itcast.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class JobMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
//        创建job任务对象
        final Job job = Job.getInstance(super.getConf(), "partition_mapreduce");
//        对job进行配置 8个步骤
//        设置输入类，文件路径
        job.setInputFormatClass(TextInputFormat.class);
//        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/input"));
        TextInputFormat.addInputPath(job,new Path("file:///D:\\input"));
//        设置mapper类和数据类型 k2 v2
        job.setMapperClass(PartitionMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

//        指定分区类
        job.setPartitionerClass(MyPartitioner.class);
//        4,5,6步骤默认
//        第7步
//        指定reducer和数据类型 k3 v3
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//      输出类和输出路径
        job.setOutputFormatClass(TextOutputFormat.class);
//        设置reducetask个数
        job.setNumReduceTasks(2);
//        TextOutputFormat.setOutputPath(job,new Path("hdfs://node01:8020/out/partition_out"));
        TextOutputFormat.setOutputPath(job,new Path("file:///D:\\out\\partition_out11"));

//        等待任务结束
        final boolean b = job.waitForCompletion(true);

        return b? 0:1;
    }

    public static void main(String[] args) throws Exception {
        final Configuration configuration = new Configuration();
        final int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}

