package com.spencer.distributed_job_scheduler.handlers;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.repository.JobRepository;
import com.spencer.distributed_job_scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

@Component
@RequiredArgsConstructor
public class JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);
    private static final String WORK_QUEUE = "scheduler:work";

    private final JobRepository jobRepository;
    private final JobService jobService;
    private final StringRedisTemplate redis;

    @Scheduled(fixedDelayString = "${scheduler.poller.delay-ms:1000}")
    public void pollAndDispatch() {
        Optional<Job> potentialJob = jobRepository.findTopByStatusAndScheduledAtBeforeOrderByScheduledAtAsc(JobStatus.PENDING, Instant.now());

        if (potentialJob.isEmpty()) {
            return;
        }

        Job job = potentialJob.get();

        // db set new status
        try {
            // set in DB to be 'RUNNING' (claim)
            jobService.markRunning(job);
        } catch (Exception ex) {
            logger.error("Failed to claim job {}: {}", job.getId(), ex.getMessage(), ex);
            return;
        }

        // redis dispatch
        try {
            redis.opsForList().leftPush(WORK_QUEUE, job.getId().toString());
        } catch (Exception ex) {
            logger.error("Failed to push job {} to Redis work queue {}: {}", job.getId(), WORK_QUEUE, ex.getMessage(), ex);

            // revert if not successful
            job.setStatus(JobStatus.PENDING);
            job.setStartedAt(null);
            jobRepository.save(job);
        }
    }
}
