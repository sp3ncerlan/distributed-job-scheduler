package com.spencer.distributed_job_scheduler.executor;

import com.spencer.distributed_job_scheduler.model.Job;

public interface JobExecutor {
    String getJobType();

    void execute(Job job) throws Exception;
}
