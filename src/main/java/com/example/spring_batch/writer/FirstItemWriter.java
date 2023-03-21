package com.example.spring_batch.writer;

import com.example.spring_batch.model.StudentCsv;
import com.example.spring_batch.model.StudentJdbc;
import com.example.spring_batch.model.StudentJson;
import com.example.spring_batch.model.StudentXml;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class FirstItemWriter implements ItemWriter<StudentJdbc> {

    // the length of list be based on chunk size that we have defined for example if chunk size is 3
    // the size of list will be 3.
    // we will not get items one by one in writer.

    @Override
    public void write(List<? extends StudentJdbc> list) throws Exception {

        System.out.println("Inside item writer");

        // writing object to console
        list.forEach(System.out::println);

    }
}
