package com.ouxiaojie.kafkaflink.operator;

import com.ouxiaojie.kafkaflink.entity.Student;
import org.apache.flink.api.common.functions.RichFilterFunction;

public class MyFilterFunction extends RichFilterFunction<Student> {
    @Override
    public boolean filter(Student student) throws Exception {
        if(student.getClassId() < 1 || student.getClassId() > 12){
            return false;
        }
        return true;
    }
}
