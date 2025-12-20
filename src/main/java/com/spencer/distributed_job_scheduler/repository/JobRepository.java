package com.spencer.distributed_job_scheduler.repository;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<Job> findTopByStatusAndScheduledAtBeforeOrderByScheduledAtAsc(JobStatus status, Instant time);
}
