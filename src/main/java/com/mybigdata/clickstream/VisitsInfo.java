package com.mybigdata.clickstream;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mybigdata.dataparser.PageViewsParser;
import com.mybigdata.dataparser.VisitsInfoParser;


public class VisitsInfo {

    public static class visitMapper extends Mapper<Object,Text,Text,Text> {

        private Text word = new Text();

        public void map(Object key,Text value,Context context) {

            String line = value.toString();
            String[] webLogContents = line.split(" ");

            //根据session来分组
            word.set(webLogContents[2]);
            try {
                context.write(word,value);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static class visitReducer extends Reducer<Text, Text, Text, NullWritable>{

        private Text content = new Text();
        private NullWritable v = NullWritable.get();
        VisitsInfoParser visitsParser = new VisitsInfoParser();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        PageViewsParser pageViewsParser = new PageViewsParser();
        Map<String,Integer> viewedPagesMap = new HashMap<String,Integer>();

        String entry_URL = "";
        String leave_URL = "";
        int total_visit_pages = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //将session所对应的所有浏览记录按时间排序
            ArrayList<String> browseInfoGroup  = new ArrayList<String>();
            for(Text browseInfo : values) {
                browseInfoGroup.add(browseInfo.toString());
            }
            Collections.sort(browseInfoGroup,new Comparator<String>() {

                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                public int compare(String browseInfo1, String browseInfo2) {
                    String dateStr1 = browseInfo1.split(" ")[0] + " " + browseInfo1.split(" ")[1];
                    String dateStr2 = browseInfo2.split(" ")[0] + " " + browseInfo2.split(" ")[1];
                    Date date1;
                    Date date2;
                    try {
                        date1 = sdf.parse(dateStr1);
                        date2 = sdf.parse(dateStr2);
                        if(date1 == null && date2 == null) return 0;
                        return date1.compareTo(date2);
                    } catch (ParseException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        return 0;
                    }
                }
            });

            //统计该session访问的总页面数,第一次进入的页面，跳出的页面
            for(String browseInfo : browseInfoGroup) {

                String[] browseInfoStrArr = browseInfo.split(" ");
                String curVisitURL = browseInfoStrArr[3];
                Integer curVisitURLInteger = viewedPagesMap.get(curVisitURL);
                if(curVisitURLInteger == null) {
                    viewedPagesMap.put(curVisitURL, 1);
                }
            }
            total_visit_pages = viewedPagesMap.size();
            String visitsInfo = visitsParser.parser(browseInfoGroup, total_visit_pages+"");
            content.set(visitsInfo);
            try {
                context.write(content,v);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://192.168.1.161:9000");

        Job job = Job.getInstance(conf);

        job.setJarByClass(VisitsInfo.class);
        ((JobConf)job.getConfiguration()).setJar("D:\\myMapReduce\\target\\myMapReduce-1.0-SNAPSHOT.jar");

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(visitMapper.class);
        job.setReducerClass(visitReducer.class);

        //指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        Date curDate = new Date();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String dateStr = sdf.format(curDate);

        //指定job的输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("/tmp/clickstream/sessiondata/"+dateStr+"/*"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("/tmp/clickstream/visitsinfo/"+dateStr+"/"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
