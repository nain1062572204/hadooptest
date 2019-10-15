package com.wang.test;

import com.wang.entity.Course;
import com.wang.entity.Student;
import com.wang.utils.HbaseUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HBaseUtilsTest {
    @Test
    public void testCreateTable(){
        HbaseUtils.createTable("student-test",new String[]{"Id","Info","Course"});
    }
    @Test
    public void testGetData(){
        HbaseUtils.getNoDealData("student-test");
    }
    @Test
    public void testDelete(){
        HbaseUtils.delete("student-test");
    }
    @Test
    public void insert(){
        List<Course> courses=new ArrayList<>();
        courses.add(new Course("Java","80"));
        courses.add(new Course("Python","99"));
        courses.add(new Course("JavaScript","60"));
        HbaseUtils.insert("student-test",new Student("s001","王念","男",courses));
    }
}
