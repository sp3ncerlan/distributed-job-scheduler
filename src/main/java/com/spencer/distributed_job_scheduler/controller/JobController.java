package com.spencer.distributed_job_scheduler.controller;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.scheduler.JobScheduler;
import com.spencer.distributed_job_scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
public class JobController {

    private final JobService jobService;

    @PostMapping
    public ResponseEntity<Job> submitJob(@RequestBody Job job) {
        if (job.getStatus() == null) {
            job.setStatus(JobStatus.PENDING);
        }

        if (job.getScheduledAt() == null) {
            job.setScheduledAt(Instant.now());
        }

        Job savedJob = jobService.createJob(job);

        return ResponseEntity.accepted().body(savedJob);
    }

    @GetMapping("/{id}")
    public ResponseEntity<Job> getJob(@PathVariable UUID id) {
        return jobService.getJob(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public ResponseEntity<List<Job>> getAllJobs() {
        return ResponseEntity.ok(jobService.getAllJobs());
    }
}
