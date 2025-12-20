package com.spencer.distributed_job_scheduler.handlers;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.service.JobService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;

@Component
@RequiredArgsConstructor
public class JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);
    private static final String WORK_QUEUE = "scheduler:work";

    private final JobService jobService;
    private final StringRedisTemplate redis;

    @Scheduled(fixedDelayString = "${scheduler.poller.delay-ms:1000}")
    public void pollAndEnqueue() {
        logger.info("pollAndEnqueue: triggered");

        Optional<Job> claimed = jobService.claimNextDueJob();
        if (claimed.isEmpty()) {
            logger.info("pollAndEnqueue: no pending jobs found");
            return;
        }

        Job job = claimed.get();
        logger.info("pollAndEnqueue: job {} claimed, enqueueing to Redis...", job.getId());

        try {
            redis.opsForList().leftPush(WORK_QUEUE, job.getId().toString());
            logger.info("pollAndEnqueue: job {} enqueued to {}", job.getId(), WORK_QUEUE);
        } catch (Exception ex) {
            logger.error("pollAndEnqueue: failed to push job {} to Redis: {}", job.getId(), ex.getMessage(), ex);
            // revert to pending if enqueue fails
            try {
                jobService.markStatus(job, JobStatus.PENDING);
                logger.info("pollAndEnqueue: job {} reverted to PENDING", job.getId());
            } catch (Exception e) {
                logger.error("pollAndEnqueue: failed to revert job {}: {}", job.getId(), e.getMessage(), e);
            }
        }
    }
}
