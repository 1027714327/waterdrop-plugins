package com.pagoda.waterdrop;


import scala.Tuple2;
import org.spark_project.guava.base.Joiner;
import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author 刘唐荣
 * @version 1.0.0
 * @ClassName Tools.java
 * @Description TODO
 * @createTime 2020-02-27 22:46:00
 * https://my.oschina.net/bankofchina/blog/3057689
 */
public class Tools implements Serializable {
    private static Pattern MODEL_PATTERN = Pattern.compile("\\$\\{\\w+(\\+|\\-)*\\w+\\,{0,1}\\w+\\}");

    public static void main(String[] args){
        String preSql = "select * from state.f_mem_rchg_dtl_tmp where part_date >='${nowDate,yyyyMMdd}'";
        String   timeFormat="yyyyMMdd";
        List<String> keys= Tools.getTemplateKey(preSql);
        Map<String, Object> pamra = Tools.processTemplateKey(keys, timeFormat);
        String newPreSql = Tools.processTemplate(preSql,pamra );
        System.out.println(newPreSql);
        System.out.println(Joiner.on(",").skipNulls().join("id","i2"));
        System.out.println("c1".split(" as | AS ")[0].trim());
    }
    /**
     * 模板替换
     *
     * @param template
     * @param params
     * @return
     */
    public static String processTemplate(String template, Map<String, Object> params) {
        StringBuffer sb = new StringBuffer();
        Matcher m = MODEL_PATTERN.matcher(template);
        while (m.find()) {
            String param = m.group();
            Object value = params.get(param.substring(2, param.length() - 1));
            m.appendReplacement(sb, value == null ? "" : value.toString());
        }
        m.appendTail(sb);
        return sb.toString();
    }

    /**
     * 获取模板key值
     *
     * @param template
     * @return
     */
    public static List<String> getTemplateKey(String template) {
        List<String> keys = new ArrayList<>();
        Matcher m = MODEL_PATTERN.matcher(template);
        while (m.find()) {
            String param = m.group();
            keys.add(param.substring(2, param.length() - 1));
        }
        return keys;
    }

    /**
     * @param keys
     * @param dateFormat
     * @return
     */
    public static Map<String, Object> processTemplateKey(List<String> keys, String dateFormat) {
        Map map = new HashMap();
        keys.stream().forEach(key -> map.put(key, compileDateStr(key, dateFormat)));
        return map;
    }

    /**
     * 格式{bizdate-1d}---使用默认
     * 格式{bizdate-1d,yyyyMMddHH}---使用指定
     * @param key
     * @param dateFormat
     * @return
     */
    private static String compileDateStr(String key, String dateFormat) {
        String dateStr = "";
        String[] rootKey = key.split(",");
        DateTimeFormatter df = DateTimeFormatter.ofPattern(dateFormat);
        if(rootKey.length>1){
            df = DateTimeFormatter.ofPattern(rootKey[1].trim());
        }
        boolean addFlag = rootKey[0].contains("+");
        boolean subFlag = rootKey[0].contains("-");
        String[] keyArr = rootKey[0].split("\\+|\\-");
        Tuple2<Boolean, LocalDateTime> localDateTimeTuple2 = getDate(keyArr[0]);
        if (localDateTimeTuple2._1.booleanValue() == false) {
            return dateStr;
        }
        LocalDateTime localDateTime = localDateTimeTuple2._2;

        if (addFlag) {
            String offsetStr = keyArr[1].trim().toLowerCase();
            long offset;
            if (offsetStr.contains("s")) {
                offset = Long.valueOf(offsetStr.replaceAll("s", ""));
                dateStr = localDateTime.plusSeconds(offset).format(df);
            } else if (offsetStr.contains("m")) {
                offset = Long.valueOf(offsetStr.replaceAll("m", ""));
                dateStr = localDateTime.plusMinutes(offset).format(df);
            } else if (offsetStr.contains("h")) {
                offset = Long.valueOf(offsetStr.replaceAll("h", ""));
                dateStr = localDateTime.plusHours(offset).format(df);
            } else if (offsetStr.contains("d")) {
                offset = Long.valueOf(offsetStr.replaceAll("d", ""));
                dateStr = localDateTime.plusDays(offset).format(df);
            } else {
                dateStr = localDateTime.format(df);
            }
        } else if (subFlag) {
            String offsetStr = keyArr[1].trim().toLowerCase();
            long offset;
            if (offsetStr.contains("s")) {
                offset = Long.valueOf(offsetStr.replaceAll("s", ""));
                dateStr = localDateTime.minusSeconds(offset).format(df);
            } else if (offsetStr.contains("m")) {
                offset = Long.valueOf(offsetStr.replaceAll("m", ""));
                dateStr = localDateTime.minusMinutes(offset).format(df);
            } else if (offsetStr.contains("h")) {
                offset = Long.valueOf(offsetStr.replaceAll("h", ""));
                dateStr = localDateTime.minusHours(offset).format(df);
            } else if (offsetStr.contains("d")) {
                offset = Long.valueOf(offsetStr.replaceAll("d", ""));
                dateStr = localDateTime.minusDays(offset).format(df);
            } else {
                dateStr = localDateTime.format(df);
            }
        } else {
            dateStr = localDateTime.format(df);
        }
        return dateStr;
    }

    /**
     * @param dateKey
     * @return
     */
    private static Tuple2<Boolean, LocalDateTime> getDate(String dateKey) {
        String nowDate = "nowDate";
        String bizDate = "bizDate";
        LocalDateTime date = LocalDateTime.now();
        if (nowDate.equals(dateKey)) {
            return new Tuple2(true, date);
        } else if (bizDate.equals(dateKey)) {
            date = date.minusDays(1);
            return new Tuple2(true, date);
        } else {
            return new Tuple2(false, null);
        }
    }
}
