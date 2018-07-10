package com.aura.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

public class Utils {
    public static String getKey(String line){
        HashMap<String, String> map = new HashMap<String,String>();
        map.put("0","其他");
        map.put("1","电视剧");
        map.put("2","电影");
        map.put("3","综艺");
        map.put("4","动漫");
        map.put("5","纪录片");
        map.put("6","游戏");
        map.put("7","资讯");
        map.put("8","娱乐");
        map.put("9","财经");
        map.put("10","网络电影");
        map.put("11","片花");
        map.put("12","音乐");
        map.put("13","军事");
        map.put("14","教育");
        map.put("15","体育");
        map.put("16","儿童");
        map.put("17","旅游");
        map.put("18","时尚");
        map.put("19","生活");
        map.put("20","汽车");
        map.put("21","搞笑");
        map.put("22","广告");
        map.put("23","原创");
         //获取到品类ID
        String categoryid = line.split("&")[9].split("/")[4];
        //获取到品类的名称
        String name = map.get(categoryid);
        //获取用户访问数据的时间
        String stringTime = line.split("&")[8].split("=")[1];
        //获取日期
        String date = getDay(Long.valueOf(stringTime));

        return date+"_"+name;
    }

    public static String getDay(long time){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        return simpleDateFormat.format(new Date(time));
    }
}
