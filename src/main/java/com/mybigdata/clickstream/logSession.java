package com.mybigdata.clickstream;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.UUID;

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

import com.mybigdata.dataparser.SessionParser;
import com.mybigdata.dataparser.WebLogParser;
import com.mybigdata.javabean.WebLogSessionBean;

public class logSession {

    public static class sessionMapper extends Mapper<Object,Text,Text,Text> {

        private Text IPAddr = new Text();
        private Text content = new Text();
        private NullWritable v = NullWritable.get();
        WebLogParser webLogParser = new WebLogParser();

        public void map(Object key,Text value,Context context) {

            //将一行内容转成string
            String line = value.toString();

            String[] weblogArry = line.split(" ");

            IPAddr.set(weblogArry[0]);
            content.set(line);
            try {
                context.write(IPAddr,content);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    static class sessionReducer extends Reducer<Text, Text, Text, NullWritable>{

        private Text IPAddr = new Text();
        private Text content = new Text();
        private NullWritable v = NullWritable.get();
        WebLogParser webLogParser = new WebLogParser();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        SessionParser sessionParser = new SessionParser();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Date sessionStartTime = null;
            String sessionID = UUID.randomUUID().toString();


            //将IP地址所对应的用户的所有浏览记录按时间排序
            ArrayList<WebLogSessionBean> sessionBeanGroup  = new ArrayList<WebLogSessionBean>();
            for(Text browseHistory : values) {
                WebLogSessionBean sessionBean = sessionParser.loadBean(browseHistory.toString());
                sessionBeanGroup.add(sessionBean);
            }
            Collections.sort(sessionBeanGroup,new Comparator<WebLogSessionBean>() {

                public int compare(WebLogSessionBean sessionBean1, WebLogSessionBean sessionBean2) {
                    Date date1 = sessionBean1.getTimeWithDateFormat();
                    Date date2 = sessionBean2.getTimeWithDateFormat();
                    if(date1 == null && date2 == null) return 0;
                    return date1.compareTo(date2);
                }
            });

            for(WebLogSessionBean sessionBean : sessionBeanGroup) {

                if(sessionStartTime == null) {
                    //当天日志中某用户第一次访问网站的时间
                    sessionStartTime = timeTransform(sessionBean.getTime());
                    content.set(sessionParser.parser(sessionBean, sessionID));
                    try {
                        context.write(content,v);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }

                } else {

                    Date sessionEndTime = timeTransform(sessionBean.getTime());
                    long sessionStayTime = timeDiffer(sessionStartTime,sessionEndTime);
                    if(sessionStayTime > 30 * 60 * 1000) {
                        //将当前浏览记录的时间设为下一个session的开始时间
                        sessionStartTime = timeTransform(sessionBean.getTime());
                        sessionID = UUID.randomUUID().toString();
                        continue;
                    }
                    content.set(sessionParser.parser(sessionBean, sessionID));
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

        private Date timeTransform(String time) {

            Date standard_time = null;
            try {
                standard_time = sdf.parse(time);
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return standard_time;
        }

        private long timeDiffer(Date start_time,Date end_time) {

            long diffTime = 0;
            diffTime = end_time.getTime() - start_time.getTime();

            return diffTime;
        }

    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        conf.set("fs.defaultFS", "hdfs://192.168.1.161:9000");

        Job job = Job.getInstance(conf);

        job.setJarByClass(logClean.class);
        ((JobConf)job.getConfiguration()).setJar("D:\\myMapReduce\\target\\myMapReduce-1.0-SNAPSHOT.jar");

        //指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(sessionMapper.class);
        job.setReducerClass(sessionReducer.class);

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
        FileInputFormat.setInputPaths(job, new Path("/tmp/clickstream/cleandata/"+dateStr+"/*"));
        //指定job的输出结果所在目录
        FileOutputFormat.setOutputPath(job, new Path("/tmp/clickstream/sessiondata/"+dateStr+"/"));

        //将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行

        boolean res = job.waitForCompletion(true);
        System.exit(res?0:1);

    }
}
