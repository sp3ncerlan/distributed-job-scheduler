package com.spencer.distributed_job_scheduler.service;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.repository.JobRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface JobService {
    Job createJob(Job job);

    Optional<Job> getJob(UUID id);

    List<Job> getAllJobs();

    // change DB to mark a job as 'RUNNING'
    void markRunning(Job job);
}
