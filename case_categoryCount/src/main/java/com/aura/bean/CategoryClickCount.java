package com.aura.bean;

import java.io.Serializable;

public class CategoryClickCount implements Serializable {
    //点击的品类
    private String name;
    //点击的次数
    private long count;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public CategoryClickCount(String name, long count) {
        this.name = name;
        this.count = count;
    }
}
