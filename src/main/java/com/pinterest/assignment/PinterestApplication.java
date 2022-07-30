package com.pinterest.assignment;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class PinterestApplication {

    public static void main(String[] args) {
        SpringApplication.run(PinterestApplication.class, args);
    }

    @Autowired
    private JobLauncher launcher;

    @Autowired
    private Job jobA;

    @Bean
    public CommandLineRunner justTest() {
        return args -> {
            JobParameters jobParameters =
                    new JobParametersBuilder()
                            .addLong("time", System.currentTimeMillis())
                            .toJobParameters();

            launcher.run(jobA, jobParameters);
            System.out.println("JOB Execution completed!");
        };
    }

}
