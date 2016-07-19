package com.vdian.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by silence on 16/7/19.
 */
public class PropertyUtil {
    private static final String  CONFIG_FILE_PATH = "src/main/resources/conf.properties";
    private static PropertyUtil propertyUtil;

    Properties properties = new Properties();

    private PropertyUtil(){ }

    public static PropertyUtil getInstance(){    //对获取实例的方法进行同步
        if (propertyUtil == null){
            synchronized(PropertyUtil.class){
                if (propertyUtil == null) {
                    propertyUtil = new PropertyUtil();
                    propertyUtil.refreshConf();
                }
            }
        }
        return propertyUtil;
    }

    /**
     * 刷新配置信息
     */
    public void refreshConf(){
        FileInputStream fileInputStream = null;
        try {
            File f = new File(PropertyUtil.CONFIG_FILE_PATH);
            if(!f.exists()){
                System.out.println("配置"+PropertyUtil.CONFIG_FILE_PATH+"文件不存在");
                return;
            }
            fileInputStream = new FileInputStream(f);
            properties.load(fileInputStream);
        }catch (FileNotFoundException e) {
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }finally {
            try {
                if (null != fileInputStream) { fileInputStream.close(); }
            }catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    /**
     * 通过配置名称获取对应的配置值
     * @param conf 配置名称
     * @return 配置的值
     */
    public String getConfig(String conf){
        return properties.getProperty(conf);
    }
}