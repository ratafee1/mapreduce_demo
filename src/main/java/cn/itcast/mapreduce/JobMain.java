package cn.itcast.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class JobMain extends Configured implements Tool {

//    该方法用于指定一个job任务
    @Override
    public int run(String[] strings) throws Exception {
//        创建一个job任务对象
        final Job job = Job.getInstance(super.getConf(), "wordcount");
        //        如果打包运行出错，则需要加该配置
        job.setJarByClass(JobMain.class);

//        配置job任务对象(8个步骤)
//        第一步指定文件的读取方式和读取路径
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path("hdfs://node01:8020/wordcount"));
//        TextInputFormat.addInputPath(job,new Path("file:///D:\\mapreduce\\input"));

//        第二步：指定Map阶段的处理方式
        job.setMapperClass(WordCountMapper.class);
//        设置Map截断k2 、v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

//        三，4，5，6采用默认的方式处理
//        第七步 指定reduce阶段的处理方式和数据类型
        job.setReducerClass(WordCountReducer.class);
//        设置k3和v3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

//        第八步 设置输出类型
        job.setOutputFormatClass(TextOutputFormat.class);
//        设置输出路径
        final Path path = new Path("hdfs://node01:8020/wordcount_out");
        TextOutputFormat.setOutputPath(job,path);
//        获取filesystem
        final FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), new Configuration());
//        判断目录是否存在
        final boolean exists = fileSystem.exists(path);
        if (exists){
//            删除目标目录
            fileSystem.delete(path,true);
        }
//        TextOutputFormat.setOutputPath(job,new Path( "file:///D:\\mapreduce\\output"));
//        等待任务结束
        final boolean bl = job.waitForCompletion(true);

        return bl ? 0:1;
    }

    public static void main(String[] args) throws Exception {
        final Configuration configuration = new Configuration();
//        启动job任务
        final int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }
}
