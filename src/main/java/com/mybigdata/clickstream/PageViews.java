package com.mybigdata.clickstream;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;


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
import com.mybigdata.javabean.PageViewsBean;

public class PageViews {

    public static class pageMapper extends Mapper<Object,Text,Text,Text> {

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

    public static class pageReducer extends Reducer<Text, Text, Text, NullWritable>{

        private Text session = new Text();
        private Text content = new Text();
        private NullWritable v = NullWritable.get();
        PageViewsParser pageViewsParser = new PageViewsParser();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //上一条记录的访问信息
        PageViewsBean lastStayPageBean = null;
        Date lastVisitTime = null;

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            //将session所对应的所有浏览记录按时间排序
            ArrayList<PageViewsBean> pageViewsBeanGroup  = new ArrayList<PageViewsBean>();
            for(Text pageView : values) {
                PageViewsBean pageViewsBean = pageViewsParser.loadBean(pageView.toString());
                pageViewsBeanGroup.add(pageViewsBean);
            }
            Collections.sort(pageViewsBeanGroup,new Comparator<PageViewsBean>() {

                public int compare(PageViewsBean pageViewsBean1, PageViewsBean pageViewsBean2) {
                    Date date1 = pageViewsBean1.getTimeWithDateFormat();
                    Date date2 = pageViewsBean2.getTimeWithDateFormat();
                    if(date1 == null && date2 == null) return 0;
                    return date1.compareTo(date2);
                }
            });

            //计算每个页面的停留时间
            int step = 0;
            for(PageViewsBean pageViewsBean : pageViewsBeanGroup) {

                Date curVisitTime = pageViewsBean.getTimeWithDateFormat();

                if(lastStayPageBean != null) {
                    //计算前后两次访问记录相差的时间，单位是秒
                    Integer timeDiff = (int) ((curVisitTime.getTime() - lastVisitTime.getTime())/1000);
                    //根据当前记录的访问信息更新上一条访问记录中访问的页面的停留时间
                    lastStayPageBean.setStayTime(timeDiff.toString());
                }

                //更新访问记录的步数
                step++;
                pageViewsBean.setStep(step+"");
                //更新上一条访问记录的停留时间后，将当前访问记录设定为上一条访问信息记录
                lastStayPageBean = pageViewsBean;
                lastVisitTime = curVisitTime;

                //输出pageViews信息
                content.set(pageViewsParser.parser(pageViewsBean));
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
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://192.168.1.161:9000");

        Job job = Job.getInstance(conf);

        job.setJarByClass(PageViews.class);
        ((JobConf)job.getConfiguration()).setJar("D:\\myMapReduce\\target\\myMapReduce-1.0-SNAPSHOT.jar");

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(pageMapper.class);
        job.setReducerClass(pageReducer.class);

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
        FileOutputFormat.setOutputPath(job, new Path("/tmp/clickstream/pageviews/"+dateStr+"/"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
