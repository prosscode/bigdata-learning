package com.aura.dao;

import com.aura.bean.CategoryClickCount;

import java.util.List;

public interface HBaseDao {
    public void save(String tableName, String rowkey,
                     String family, String q, long value);
    public List<CategoryClickCount> count(String tableName, String rowkey);
}
