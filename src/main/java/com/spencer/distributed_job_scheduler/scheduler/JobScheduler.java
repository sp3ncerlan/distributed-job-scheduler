package com.spencer.distributed_job_scheduler.scheduler;

import com.spencer.distributed_job_scheduler.executor.JobExecutor;
import com.spencer.distributed_job_scheduler.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Component
public class JobScheduler {
    private static final Logger logger = LoggerFactory.getLogger(JobScheduler.class);

    private final Map<String, JobExecutor> executors;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    public JobScheduler(List<JobExecutor> executors) {
        this.executors = executors.stream()
                .collect(Collectors.toConcurrentMap(JobExecutor::getJobType, e -> e, (a, b) -> a));
    }

    public void scheduleNow(Job job) {
        schedule(job, 0L);
    }

    public void schedule(Job job, long delayMillis) {
        JobExecutor executor = executors.get(job.getJobType());
        if (executor == null) {
            throw new IllegalStateException("No executor registered for job type: " + job.getJobType());
        }

        scheduler.schedule(() -> {
            try {
                executor.execute(job);
            } catch (Exception ex) {
                logger.error("Job execution failed for jobId={} type={}: {}", job.getId(), job.getJobType(), ex.getMessage(), ex);
            }
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
