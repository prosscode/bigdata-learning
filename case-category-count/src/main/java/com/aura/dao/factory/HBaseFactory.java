package com.aura.dao.factory;

import com.aura.dao.HBaseDao;
import com.aura.dao.impl.HBaseImpl;

public class HBaseFactory {
    public static HBaseDao getHBaseDao(){
        return new HBaseImpl();
    }
}
