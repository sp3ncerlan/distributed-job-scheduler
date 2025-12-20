package com.spencer.distributed_job_scheduler.handlers;

import com.spencer.distributed_job_scheduler.executor.JobExecutor;
import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.repository.JobRepository;
import com.spencer.distributed_job_scheduler.service.JobService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class JobWorker {

    private static final Logger logger = LoggerFactory.getLogger(JobWorker.class);
    private static final String WORK_QUEUE = "scheduler:work";

    private final StringRedisTemplate redis;
    private final JobRepository jobRepository;
    private final JobService jobService;
    private final JobExecutor jobExecutor;

    // ExecutorService manages the lifecycle of executors
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    private Counter failedCounter;
    private Timer executionTimer;

    public JobWorker(StringRedisTemplate redis,
                     JobRepository jobRepository,
                     JobService jobService,
                     JobExecutor jobExecutor) {
        this.redis = redis;
        this.jobRepository = jobRepository;
        this.jobService = jobService;
        this.jobExecutor = jobExecutor;
    }

    // optional metric init
    @org.springframework.beans.factory.annotation.Autowired(required = false)
    public void initMetrics(MeterRegistry registry) {
        if (registry != null) {
            this.failedCounter = Counter.builder("jobs.failed.total").description("Total failed jobs").register(registry);
            this.executionTimer = Timer.builder("jobs.execution.duration").description("Job execution duration").publishPercentiles(0.5, 0.95).register(registry);
        }
    }

    @PostConstruct
    public void start() {
        // scheduler re-runs after a configured delay
        scheduler.scheduleWithFixedDelay(() -> {
                try {
                    String idString = redis.opsForList().rightPop(WORK_QUEUE, Duration.ofSeconds(5));

                    if (idString == null) {
                        return;
                    }

                    // validate UUID from queue; skip invalid values
                    UUID id;
                    try {
                        id = UUID.fromString(idString);
                    } catch (IllegalArgumentException iae) {
                        logger.warn("Invalid job id from queue, skipping: {}", idString);
                        return;
                    }

                    Optional<Job> potentialJob = jobRepository.findById(id);

                    if (potentialJob.isEmpty()) {
                        logger.warn("Received job id {} from queue but not found in DB", id);
                        return;
                    }

                    Job job = potentialJob.get();

                    // skip if already completed (duplicate in queue)
                    if (job.getStatus() == JobStatus.COMPLETED) {
                        logger.info("Job {} already completed; skipping duplicate queue entry", id);
                        return;
                    }

                    try {
                        // attempt to mark RUNNING; handle optimistic lock races
                        job.setStartedAt(Instant.now());
                        try {
                            jobService.markStatus(job, JobStatus.RUNNING);
                        } catch (ObjectOptimisticLockingFailureException oole) {
                            logger.debug("Job {} already updated by another worker when marking RUNNING; skipping", id);
                            return;
                        }

                        if (executionTimer != null) {
                            executionTimer.record(() -> {
                                try {
                                    jobExecutor.execute(job);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                            });
                        } else {
                            jobExecutor.execute(job);
                        }

                        // end time and mark COMPLETED
                        job.setFinishedAt(Instant.now());
                        try {
                            jobService.markStatus(job, JobStatus.COMPLETED);
                            logger.info("Job {} completed", id);
                        } catch (ObjectOptimisticLockingFailureException oole) {
                            // someone else updated the row (likely marked COMPLETED) — treat as already-processed
                            logger.debug("Job {} already updated by another worker when marking COMPLETED; treating as processed", id);
                        }
                    } catch (Exception ex) {
                        logger.error("Job {} execution failed: {}", id, ex.getMessage(), ex);
                        if (failedCounter != null) failedCounter.increment();
                        try {
                            jobService.markStatus(job, JobStatus.FAILED);
                        } catch (ObjectOptimisticLockingFailureException oole) {
                            logger.debug("Job {} could not be marked FAILED due to optimistic lock; it may have been updated by another worker", id);
                        }
                    }
                } catch (Exception ex) {
                    logger.error("Worker loop error: {}", ex.getMessage(), ex);
                }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        scheduler.shutdownNow();

        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                logger.warn("Scheduler did not terminate within timeout");
            }
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            logger.warn("Interrupted while waiting for scheduler shutdown", ex);
        }
    }
}
