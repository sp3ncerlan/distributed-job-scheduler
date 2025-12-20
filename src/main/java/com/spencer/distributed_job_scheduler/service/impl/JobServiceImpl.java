package com.spencer.distributed_job_scheduler.service.impl;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.repository.JobRepository;
import com.spencer.distributed_job_scheduler.service.JobService;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class JobServiceImpl implements JobService {

    private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

    private final JobRepository jobRepository;

    // test-only delay to slow down claiming for visibility; default 0
    private final long testDelayMs;

    public JobServiceImpl(JobRepository jobRepository, @Value("${scheduler.test.delay-ms:0}") long testDelayMs) {
        this.jobRepository = jobRepository;
        this.testDelayMs = testDelayMs;
    }

    @Override
    public Job createJob(Job job) {
        return jobRepository.save(job);
    }

    @Override
    public Optional<Job> getJob(UUID id) {
        return jobRepository.findById(id);
    }

    @Override
    public List<Job> getAllJobs() {
        return jobRepository.findAll();
    }

    @Override
    @Transactional
    public void markStatus(Job job, JobStatus status) {
        job.setStatus(status);
        if (status == JobStatus.RUNNING) {
            job.setStartedAt(Instant.now());
        } else if (status == JobStatus.PENDING) {
            job.setStartedAt(null);
        }
        jobRepository.save(job);
    }

    @Override
    @Transactional
    public Optional<Job> claimNextDueJob() {
        Optional<Job> opt = jobRepository.findTopByStatusAndScheduledAtBeforeOrderByScheduledAtAsc(JobStatus.PENDING, Instant.now());
        if (opt.isEmpty()) {
            logger.debug("claimNextDueJob: no candidate found");
            return Optional.empty();
        }

        Job job = opt.get();
        logger.info("claimNextDueJob: found job {}, claiming...", job.getId());

        job.setStatus(JobStatus.RUNNING);
        job.setStartedAt(Instant.now());
        try {
            String host = InetAddress.getLocalHost().getHostName();
            job.setClaimedBy(host + "-" + UUID.randomUUID());
        } catch (Exception e) {
            job.setClaimedBy("scheduler-" + UUID.randomUUID());
        }

        jobRepository.save(job);
        logger.info("claimNextDueJob: job {} marked RUNNING (startedAt={}, claimedBy={})", job.getId(), job.getStartedAt(), job.getClaimedBy());

        if (testDelayMs > 0) {
            logger.info("claimNextDueJob: sleeping {}ms for test visibility", testDelayMs);
            try {
                Thread.sleep(testDelayMs);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }

        return Optional.of(job);
    }
}
