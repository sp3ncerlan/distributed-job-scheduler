package com.spencer.distributed_job_scheduler.repository;

import com.spencer.distributed_job_scheduler.model.Job;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.UUID;

public interface JobRepository extends JpaRepository<Job, UUID> {
}
