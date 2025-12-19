package com.spencer.distributed_job_scheduler.handlers;

import com.spencer.distributed_job_scheduler.executor.JobExecutor;
import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.repository.JobRepository;
import jakarta.annotation.Nullable;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
@RequiredArgsConstructor
public class JobWorker {

    private static final Logger logger = LoggerFactory.getLogger(JobWorker.class);
    private static final String WORK_QUEUE = "scheduler:work";

    private final StringRedisTemplate redis;
    private final JobRepository jobRepository;
    private final JobExecutor jobExecutor;

    // ExecutorService manages the lifecycle of executors
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

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
                        jobExecutor.execute(job);
                        job.setStatus(JobStatus.COMPLETED);
                        jobRepository.save(job);
                        logger.info("Job {} completed", id);
                    } catch (Exception ex) {
                        logger.error("Job {} execution failed: {}", id, ex.getMessage(), ex);
                        job.setStatus(JobStatus.FAILED);
                        jobRepository.save(job);
                    }
                } catch (Exception ex) {
                    logger.error("Worker loop error: {}", ex.getMessage(), ex);
                }
        }, 0, 1, TimeUnit.SECONDS);
    }

    @PreDestroy
    public void stop() {
        scheduler.shutdown();
    }
}
