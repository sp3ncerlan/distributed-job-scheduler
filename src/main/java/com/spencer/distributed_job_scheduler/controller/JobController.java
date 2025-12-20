package com.spencer.distributed_job_scheduler.controller;

import com.spencer.distributed_job_scheduler.dto.CreateJobRequest;
import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import tools.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/jobs")
@RequiredArgsConstructor
public class JobController {

    private static final Logger logger = LoggerFactory.getLogger(JobController.class);

    private final JobService jobService;
    private final ObjectMapper objectMapper;

    @PostMapping
    public ResponseEntity<UUID> submitJob(@RequestBody CreateJobRequest jobRequest) {
        Job job = new Job();
        job.setJobType(jobRequest.getJobType());
        Instant scheduled = jobRequest.getScheduledAt() == null ? Instant.now() : jobRequest.getScheduledAt();
        job.setScheduledAt(scheduled);
        job.setStatus(JobStatus.PENDING);

        String payloadAsString = objectMapper.writeValueAsString(jobRequest.getPayload());
        job.setPayload(payloadAsString);

        jobService.createJob(job);

        return ResponseEntity.ok(job.getId());
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
