package com.spencer.distributed_job_scheduler.service;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobService {
    Job createJob(Job job);

    Optional<Job> getJob(UUID id);

    List<Job> getAllJobs();

    // change STATUS in DB
    void markStatus(Job job, JobStatus status);

    Optional<Job> claimNextDueJob();
}
