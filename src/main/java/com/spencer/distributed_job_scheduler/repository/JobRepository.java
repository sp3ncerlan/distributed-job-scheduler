package com.spencer.distributed_job_scheduler.repository;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {
    Optional<Job> findTopByStatusAndScheduledAtBeforeOrderByScheduledAtAsc(JobStatus status, Instant time);
}
