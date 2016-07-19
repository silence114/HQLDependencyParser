package com.vdian.util;

import org.junit.Test;
import static org.junit.Assert.assertEquals;
/**
 * Created by silence on 16/7/19.
 */
public class PropertyUtilTest {

    @Test
    public void getConfigTest(){
        PropertyUtil propertyUtil = PropertyUtil.getInstance();

        assertEquals(propertyUtil.getConfig("host"),"127.0.0.1");
        assertEquals(propertyUtil.getConfig("port"),"3306");
        assertEquals(propertyUtil.getConfig("username"),"hive");
        assertEquals(propertyUtil.getConfig("password"),"hive");
        assertEquals(propertyUtil.getConfig("database"),"hive_metastore");
        assertEquals(propertyUtil.getConfig("database1"),null);
    }
}
