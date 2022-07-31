package com.ouxiaojie.kafkaflink.operator;

import com.alibaba.fastjson.JSON;
import com.ouxiaojie.kafkaflink.entity.Student;
import org.apache.flink.api.common.functions.RichMapFunction;

public class MyMapFunction extends RichMapFunction<String, Student> {
    @Override
    public Student map(String s){
        try{
            return JSON.parseObject(s, Student.class);
        }catch (Exception e){
            System.out.println("看看:"+s);
            System.out.println("转换失败:"+e);
        }
        return null;
    }
}
