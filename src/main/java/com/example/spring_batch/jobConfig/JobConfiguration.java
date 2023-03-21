package com.example.spring_batch.jobConfig;

import com.example.spring_batch.model.StudentCsv;
import com.example.spring_batch.model.StudentJdbc;
import com.example.spring_batch.model.StudentJson;
import com.example.spring_batch.model.StudentXml;
import com.example.spring_batch.writer.FirstItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.*;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

import javax.sql.DataSource;
import javax.xml.bind.annotation.XmlRegistry;
import java.io.IOException;
import java.io.Writer;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

@Configuration
public class JobConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private FirstItemWriter firstItemWriter;

    @Autowired
    private DataSource dataSource;


    @Bean
    public Job firstJob() {
        return jobBuilderFactory.get("first_job")
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep())
                .build();
    }


    @Bean
    public Step firstChunkStep() {

        return stepBuilderFactory.get("first_chunk_step")
                .<StudentJdbc, StudentJdbc>chunk(3)
                //.reader(flatFileItemReader())
                //.reader(jsonJsonItemReader())
                //.reader(staxEventItemReader())
                .reader(jdbcJdbcCursorItemReader())
                //.processor()
                //.writer(flatFileItemWriter())
                .writer(jsonJsonFileItemWriter())
                .build();

    }

    // we need flat file item reader to read csv files
    @Bean
    public FlatFileItemReader<StudentCsv> flatFileItemReader() {
        FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<>();

        // set the location to file
        flatFileItemReader.setResource(new FileSystemResource("C:\\Users\\Salman Asad\\Documents\\Projects-Latest\\SpringBatchCustomReaderAndWriter\\src\\inputFiles\\students.csv"));

        DefaultLineMapper<StudentCsv> defaultLineMapper = new DefaultLineMapper<>();

        DelimitedLineTokenizer delimitedLineTokenizer = new DelimitedLineTokenizer();

        // provide names of column header in csv file
        delimitedLineTokenizer.setNames("ID", "First Name", "Last Name", "Email");

        defaultLineMapper.setLineTokenizer(delimitedLineTokenizer);

        BeanWrapperFieldSetMapper<StudentCsv> beanWrapperFieldSetMapper = new BeanWrapperFieldSetMapper<>();
        beanWrapperFieldSetMapper.setTargetType(StudentCsv.class);

        defaultLineMapper.setFieldSetMapper(beanWrapperFieldSetMapper);

        flatFileItemReader.setLineMapper(defaultLineMapper);

        // we need to tell flat file item reader to skip first line in csv file as it is common header
        flatFileItemReader.setLinesToSkip(1);

        return flatFileItemReader;
    }

    @StepScope
    @Bean
    public JsonItemReader<StudentJson> jsonJsonItemReader() {

        JsonItemReader<StudentJson> jsonJsonItemReader = new JsonItemReader<>();

        jsonJsonItemReader.setResource(new FileSystemResource("C:\\Users\\Salman Asad\\Documents\\Projects-Latest\\SpringBatchCustomReaderAndWriter\\src\\inputFiles\\students.json"));

        // use jackson to read json
        jsonJsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<StudentJson>(StudentJson.class));

        return jsonJsonItemReader;
    }

    // stax means streaming api for xml
    @Bean
    public StaxEventItemReader<StudentXml> staxEventItemReader() {

        StaxEventItemReader<StudentXml> staxEventItemReader = new StaxEventItemReader<>();

        staxEventItemReader.setResource(new FileSystemResource("C:\\Users\\Salman Asad\\Documents\\Projects-Latest\\SpringBatchCustomReaderAndWriter\\src\\inputFiles\\students.xml"));

        // set xml root element here which is student in our xml file
        staxEventItemReader.setFragmentRootElementName("student");

        // we need a marshaller to convert xml to java
        Jaxb2Marshaller jaxb2Marshaller = new Jaxb2Marshaller();
        jaxb2Marshaller.setClassesToBeBound(StudentXml.class);

        staxEventItemReader.setUnmarshaller(jaxb2Marshaller);

        return staxEventItemReader;
    }

    @Bean
    public JdbcCursorItemReader<StudentJdbc> jdbcJdbcCursorItemReader() {

        JdbcCursorItemReader<StudentJdbc> jdbcJdbcCursorItemReader
                = new JdbcCursorItemReader<>();

        jdbcJdbcCursorItemReader.setDataSource(dataSource);

        // first_name is the column name in database and in our model class we have firstName,
        // so we use alias to map the column name to our model class.
        jdbcJdbcCursorItemReader.setSql("Select id, first_name as firstName," +
                "last_name as lastName, email from student");

        BeanPropertyRowMapper<StudentJdbc> rowMapper = new BeanPropertyRowMapper<>();
        rowMapper.setMappedClass(StudentJdbc.class);

        jdbcJdbcCursorItemReader.setRowMapper(rowMapper);

        return jdbcJdbcCursorItemReader;
    }

    // model class from where we are reading
    @StepScope
    @Bean
    public FlatFileItemWriter<StudentJdbc> flatFileItemWriter() {

        FlatFileItemWriter<StudentJdbc> flatFileItemWriter =
                new FlatFileItemWriter<>();

        flatFileItemWriter.setResource(new FileSystemResource("C:\\Users\\Salman Asad\\Documents\\Projects-Latest\\SpringBatchCustomReaderAndWriter\\src\\outputFiles\\students.csv"));

        // set column headers
        flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("Id,FirstName,LastName,Email");
            }
        });

        DelimitedLineAggregator<StudentJdbc> delimitedLineAggregator = new DelimitedLineAggregator<>();

        BeanWrapperFieldExtractor<StudentJdbc> beanWrapperFieldExtractor = new BeanWrapperFieldExtractor<>();
        beanWrapperFieldExtractor.setNames(new String[]{"id", "firstName", "lastName", "email"});

        delimitedLineAggregator.setFieldExtractor(beanWrapperFieldExtractor);

        flatFileItemWriter.setLineAggregator(delimitedLineAggregator);

        flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {
            @Override
            public void writeFooter(Writer writer) throws IOException {
                writer.write("Created at " + new Date());
            }
        });


        return flatFileItemWriter;
    }

    @Bean
    public JsonFileItemWriter<StudentJdbc> jsonJsonFileItemWriter() {

        Resource resource = new FileSystemResource("C:\\Users\\Salman Asad\\Documents\\Projects-Latest\\SpringBatchCustomReaderAndWriter\\src\\outputFiles\\students.json");

        JsonObjectMarshaller<StudentJdbc> jsonObjectMarshaller = new JacksonJsonObjectMarshaller();


        JsonFileItemWriter<StudentJdbc> jdbcJsonFileItemWriter =
                new JsonFileItemWriter<>(resource, jsonObjectMarshaller);


        return jdbcJsonFileItemWriter;
    }
}
