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

    private MeterRegistry meterRegistry;
    private Counter completedCounter;
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
        this.meterRegistry = registry;
        if (this.meterRegistry != null) {
            this.completedCounter = Counter.builder("jobs.completed.total").description("Total completed jobs").register(meterRegistry);
            this.failedCounter = Counter.builder("jobs.failed.total").description("Total failed jobs").register(meterRegistry);
            this.executionTimer = Timer.builder("jobs.execution.duration").description("Job execution duration").publishPercentiles(0.5, 0.95).register(meterRegistry);
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

                    UUID id = UUID.fromString(idString);
                    Optional<Job> potentialJob = jobRepository.findById(id);

                    if (potentialJob.isEmpty()) {
                        logger.warn("Received job id {} from queue but not found in DB", id);
                        return;
                    }

                    Job job = potentialJob.get();
                    try {
                        // start time
                        job.setStartedAt(Instant.now());

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

                        // end time
                        job.setFinishedAt(Instant.now());
                        jobService.markStatus(job, JobStatus.COMPLETED);
                        if (completedCounter != null) completedCounter.increment();
                        logger.info("Job {} completed", id);
                    } catch (Exception ex) {
                        logger.error("Job {} execution failed: {}", id, ex.getMessage(), ex);
                        if (failedCounter != null) failedCounter.increment();
                        jobService.markStatus(job, JobStatus.FAILED);
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
