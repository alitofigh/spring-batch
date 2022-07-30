package com.pinterest.assignment.config;

/* created by Ali Tofigh  7/27/2022 10:11 PM */

import com.pinterest.assignment.TitleAkaListener;
import com.pinterest.assignment.domains.TitleAkas;
import com.pinterest.assignment.helper.BlankLineRecordSeparatorPolicy;
import com.pinterest.assignment.helper.TitleAkasMapper;
import com.pinterest.assignment.repository.TitleAkasRepository;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

/*@Configuration
@EnableBatchProcessing*/
public class BatchConfig {

    @Autowired
    TitleAkasRepository titleAkasRepository;

    @Autowired
    private StepBuilderFactory sbf;

    @Autowired
    private JobBuilderFactory jbf;


    @Bean
    public FlatFileItemReader<TitleAkas> reader() {
        FlatFileItemReader<TitleAkas> reader= new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("/data/title_akas.tsv"));
        reader.setLineMapper(new DefaultLineMapper<TitleAkas>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setDelimiter(DELIMITER_TAB);
                setNames("nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles");
                setStrict(false);
            }});

            setFieldSetMapper(new TitleAkasMapper());
        }});

        reader.setLinesToSkip(1);

        reader.setRecordSeparatorPolicy(new BlankLineRecordSeparatorPolicy());
        return reader;
    }

    @Bean
    public ItemWriter<TitleAkas> writer() {
        return titleAkases -> {
            System.out.println("saving titleAkas records " + titleAkases);
            titleAkasRepository.saveAll(titleAkases);
        };
    }

    @Bean
    public ItemProcessor<TitleAkas, TitleAkas> processor() {
        return titleAkas -> {
            System.out.println(titleAkas.getPrimaryName());
            return titleAkas;
        };
    }

    @Bean
    public JobExecutionListener listener() {
        return new TitleAkaListener();
    }

    @Bean
    public Step stepA() {
        return sbf.get("stepA")
                .<TitleAkas,TitleAkas>chunk(100)
                .reader(reader())
                .processor(processor())
                .writer(writer())
                .build();
    }

    @Bean
    public Job jobA() {
        return jbf.get("jobA")
                .incrementer(new RunIdIncrementer())
                .listener(listener())
                .start(stepA())
                .build();
    }
}
