package com.mybigdata.dataparser;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.mybigdata.javabean.WebLogBean;
/**
 *   用正则表达式匹配出合法的日志记录
 *
 *
 */
public class WebLogParser {

    public String parser(String weblog_origin) {

        WebLogBean weblogbean = new WebLogBean();

        //&nbsp;获取IP地址
        Pattern IPPattern = Pattern.compile("\\d+.\\d+.\\d+.\\d+");
        Matcher IPMatcher = IPPattern.matcher(weblog_origin);
        if(IPMatcher.find()) {
            String IPAddr = IPMatcher.group(0);
            weblogbean.setIP_addr(IPAddr);
        } else {
            return "";
        }
        //&nbsp;获取时间信息
        Pattern TimePattern = Pattern.compile("\\[(.+)\\]");
        Matcher TimeMatcher = TimePattern.matcher(weblog_origin);
        if(TimeMatcher.find()) {
            String time = TimeMatcher.group(1);
            String[] cleanTime = time.split(" ");
            weblogbean.setTime(cleanTime[0]);
        } else {
            return "";
        }

        //获取其余请求信息
        Pattern InfoPattern = Pattern.compile(
                "(\\\"[POST|GET].+?\\\") (\\d+) (\\d+).+?(\\\".+?\\\") (\\\".+?\\\")");

        Matcher InfoMatcher = InfoPattern.matcher(weblog_origin);
        if(InfoMatcher.find()) {

            String requestInfo = InfoMatcher.group(1).replace('\"',' ').trim();
            String[] requestInfoArry = requestInfo.split(" ");
            weblogbean.setMethod(requestInfoArry[0]);
            weblogbean.setRequest_URL(requestInfoArry[1]);
            weblogbean.setRequest_protocol(requestInfoArry[2]);
            String status_code = InfoMatcher.group(2);
            weblogbean.setRespond_code(status_code);

            String respond_data = InfoMatcher.group(3);
            weblogbean.setRespond_data(respond_data);

            String request_come_from = InfoMatcher.group(4).replace('\"',' ').trim();
            weblogbean.setRequst_come_from(request_come_from);

            String browserInfo = InfoMatcher.group(5).replace('\"',' ').trim();
            weblogbean.setBrowser(browserInfo);
        } else {
            return "";
        }

        return weblogbean.toString();
    }

}
