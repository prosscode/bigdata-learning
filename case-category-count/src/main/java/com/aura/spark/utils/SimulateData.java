package com.aura.spark.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class SimulateData {
    public static void main(String[] args) {
        BufferedWriter bw=null;
        try {
             bw = new BufferedWriter(new FileWriter("D:\\data.txt"));
            int i=0;
            while(i < 20000){
                long time = System.currentTimeMillis();
                int categoryid = new Random().nextInt(23);
                bw.write("ver=1&en=e_pv&pl=website&sdk=js&b_rst=1920*1080&u_ud=12GH4079-223E-4A57-AC60-C1A04D8F7A2F&l=zh-CN&u_sd=8E9559B3-DA35-44E1-AC98-85EB37D1F263&c_time="+time+"&p_url=http://list.iqiyi.com/www/"+categoryid+"/---.html");
                bw.newLine();
                i++;
            }
        } catch (Exception e){

        }finally {
            try {
                bw.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }
}
