package com.vdian.util;

import static org.junit.Assert.*;

import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by silence on 16/7/18.
 */
public class HQLDependencyParserTest {
//    HQLDependencyParser lep = null;
//    @BeforeClass
//    public static void init(){
//        HQLDependencyParser lep = new HQLDependencyParser();
//    }


    @Test
    public void parseQueryTest(){
        // 测试用例 2
        HQLDependencyParser lep = new HQLDependencyParser();
        lep.parseQuery("insert into table test select * from di.user_info");

        HashMap<String,ArrayList> input = lep.getInputTableList();
        ArrayList<String> output = lep.getOutputTableList();

        assertEquals(1,input.size());
        assertEquals(1,output.size());
    }
}
