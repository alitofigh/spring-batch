package com.pinterest.assignment;

/* created by Ali Tofigh  7/28/2022 12:08 AM */

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;


public class TitleAkaListener implements JobExecutionListener {
    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Job started at: " + jobExecution.getStartTime());
        System.out.println("Status of the job: " + jobExecution.getStatus());
    }

    @Override
    public void afterJob(JobExecution jobExecution) {
        System.out.println("Job ended at: " + jobExecution.getEndTime());
        System.out.println("Status of the job: " + jobExecution.getStatus());
    }
}
