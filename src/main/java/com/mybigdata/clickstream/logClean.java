package com.mybigdata.clickstream;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mybigdata.dataparser.WebLogParser;


public class logClean  {

    public static class cleanMap extends Mapper<Object,Text,Text,NullWritable> {

        private NullWritable v = NullWritable.get();
        private Text word = new Text();
        WebLogParser webLogParser = new WebLogParser();

        public void map(Object key,Text value,Context context) {

            //将一行内容转成string
            String line = value.toString();

            String cleanContent = webLogParser.parser(line);

            if(cleanContent != "") {
                word.set(cleanContent);
                try {
                    context.write(word,v);
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://192.168.1.161:9000");

        Job job = Job.getInstance(conf);

        job.setJarByClass(logClean.class);
        ((JobConf)job.getConfiguration()).setJar("D:\\myMapReduce\\target\\myMapReduce-1.0-SNAPSHOT.jar");

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(cleanMap.class);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //指定job的输入原始文件所在目录
        Date curDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = sdf.format(curDate);
        FileInputFormat.setInputPaths(job, new Path("/tmp/flume/events/" + dateStr + "/*"));

        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("/tmp/clickstream/cleandata/"+dateStr+"/"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }

}
