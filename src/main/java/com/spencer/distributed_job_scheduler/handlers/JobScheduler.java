package com.spencer.distributed_job_scheduler.handlers;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.service.JobService;
import com.spencer.distributed_job_scheduler.redis.RedisDistributedLock;
import org.springframework.beans.factory.annotation.Value;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Optional;
import java.time.Duration;

@Component
@RequiredArgsConstructor
public class JobScheduler {

    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);
    private static final String WORK_QUEUE = "scheduler:work";
    private static final String LOCK_KEY = "scheduler:lock:poller";

    private final JobService jobService;
    private final StringRedisTemplate redis;
    private final RedisDistributedLock lock;

    @Value("${scheduler.lock-ttl-seconds:30}")
    private int lockTtlSeconds;

    @Scheduled(fixedDelayString = "${scheduler.poller.delay-ms:1000}")
    public void pollAndEnqueue() {
        logger.info("pollAndEnqueue: triggered");

        // try to acquire a distributed lock so only one scheduler polls/enqueues at a time
        String lockToken = null;
        try {
            lockToken = lock.tryAcquire(LOCK_KEY, Duration.ofSeconds(lockTtlSeconds));
            if (lockToken == null) {
                logger.debug("pollAndEnqueue: another instance holds the poller lock, skipping this cycle");
                return;
            }

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
        } finally {
            if (lockToken != null) {
                try {
                    boolean released = lock.release(LOCK_KEY, lockToken);
                    if (!released) {
                        logger.warn("pollAndEnqueue: failed to release lock {}", LOCK_KEY);
                    }
                } catch (Exception e) {
                    logger.error("pollAndEnqueue: error releasing lock {}: {}", LOCK_KEY, e.getMessage(), e);
                }
            }
        }
    }
}
