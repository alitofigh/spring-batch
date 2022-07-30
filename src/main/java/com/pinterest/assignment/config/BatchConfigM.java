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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.builder.JdbcPagingItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableBatchProcessing
public class BatchConfigM {

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
    public Job multithreadedJob(JobBuilderFactory jbf) throws Exception {
        return jbf
                .get("Multithreaded JOB")
                .incrementer(new RunIdIncrementer())
                .flow(multithreadedManagerStep(null))
                .end()
                .build();
    }

    @Bean
    public Step multithreadedManagerStep(StepBuilderFactory sbf) throws Exception {
        return sbf
                .get("Multithreaded : Read -> Process -> Write ")
                .<TitleAkas, TitleAkas>chunk(1000)
                .reader(multithreadedcReader(null))
                .processor(multithreadedchProcessor())
                .writer(multithreadedcWriter())
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(64);
        executor.setMaxPoolSize(64);
        executor.setQueueCapacity(64);
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setThreadNamePrefix("MultiThreaded-");
        return executor;
    }

    @Bean
    public ItemProcessor<TitleAkas, TitleAkas> multithreadedchProcessor() {
        return (transaction) -> {
            Thread.sleep(1);
            //og.info(Thread.currentThread().getName());
            return transaction;
        };
    }

    @Bean
    public ItemReader<TitleAkas> multithreadedcReader(DataSource dataSource) throws Exception {

        return new JdbcPagingItemReaderBuilder<TitleAkas>()
                .name("Reader")
                .dataSource(dataSource)
                .selectClause("SELECT * ")
                .fromClause("FROM transactions ")
                .whereClause("WHERE ID <= 1000000 ")
                .sortKeys(Collections.singletonMap("ID", Order.ASCENDING))
                .rowMapper(new TransactionVORowMapper())
                .build();
    }

    @Bean
    public FlatFileItemWriter<TitleAkas> multithreadedcWriter() {

        return new FlatFileItemWriterBuilder<TitleAkas>()
                .name("Writer")
                .append(false)
                .resource(new FileSystemResource("transactions.txt"))
                .lineAggregator(new DelimitedLineAggregator<TitleAkas>() {
                    {
                        setDelimiter("\t");

                        setFieldExtractor(new BeanWrapperFieldExtractor<TitleAkas>() {
                            {

                                setNames(new String[]{"nconst", "primaryName", "birthYear", "deathYear", "primaryProfession", "knownForTitles"});
                            }
                        });
                    }
                })
                .build();
    }
}
