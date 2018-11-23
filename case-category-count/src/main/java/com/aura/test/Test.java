package com.aura.test;

import com.aura.bean.CategoryClickCount;
import com.aura.dao.HBaseDao;
import com.aura.dao.factory.HBaseFactory;

import java.util.List;

public class Test {
    public static void main(String[] args) {
        HBaseDao hBaseDao = HBaseFactory.getHBaseDao();
//        hBaseDao.save("aura",
//                "2018-05-23_电影","f","name",10L);
//        hBaseDao.save("aura",
//                "2018-05-23_电影","f","name",20L);
//        hBaseDao.save("aura",
//                "2018-05-21_电视剧","f","name",11L);
//        hBaseDao.save("aura",
//                "2018-05-21_电视剧","f","name",24L);
//
//        hBaseDao.save("aura",
//                "2018-05-23_电视剧","f","name",110L);
//        hBaseDao.save("aura",
//                "2018-05-23_电视剧","f","name",210L);


        List<CategoryClickCount> list = hBaseDao.count("aura", "2018-05-22");
        for(CategoryClickCount cc:list){
            System.out.println(cc.getName() + "  "+ cc.getCount());
        }

    }
}
