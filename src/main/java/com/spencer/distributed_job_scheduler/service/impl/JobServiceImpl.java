package com.spencer.distributed_job_scheduler.service.impl;

import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.repository.JobRepository;
import com.spencer.distributed_job_scheduler.service.JobService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.transaction.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.InetAddress;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
public class JobServiceImpl implements JobService {

    private static final Logger logger = LoggerFactory.getLogger(JobServiceImpl.class);

    private final JobRepository jobRepository;

    // test-only delay to slow down claiming for visibility; default 0
    private final long testDelayMs;

    @Autowired(required = false)
    private MeterRegistry meterRegistry;

    private Counter claimedCounter;
    private Counter completedCounter;
    private Timer claimTimer;

    public JobServiceImpl(JobRepository jobRepository, @Value("${scheduler.test.delay-ms:0}") long testDelayMs) {
        this.jobRepository = jobRepository;
        this.testDelayMs = testDelayMs;
    }

    @Autowired
    public void initMetrics(MeterRegistry registry) {
        this.meterRegistry = registry;
        if (this.meterRegistry != null) {
            this.claimedCounter = Counter.builder("jobs.claimed.total")
                    .description("Total jobs claimed by schedulers")
                    .register(meterRegistry);

            this.completedCounter = Counter.builder("jobs.completed.total")
                    .description("Total jobs completed")
                    .register(meterRegistry);

            this.claimTimer = Timer.builder("jobs.claim.duration")
                    .description("Duration to claim a job")
                    .publishPercentiles(0.5, 0.95)
                    .register(meterRegistry);
        }
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
        // reload the current DB state to avoid using a potentially stale entity instance
        Optional<Job> currentOpt = jobRepository.findById(job.getId());
        if (currentOpt.isEmpty()) {
            // nothing to do if the job no longer exists
            return;
        }

        Job current = currentOpt.get();
        JobStatus previous = current.getStatus();

        // if there's no effective change, skip
        if (previous == status) {
            return;
        }

        // apply transition on the fresh entity
        current.setStatus(status);
        if (status == JobStatus.RUNNING) {
            current.setStartedAt(Instant.now());
        } else if (status == JobStatus.PENDING) {
            current.setStartedAt(null);
        } else if (status == JobStatus.COMPLETED) {
            current.setFinishedAt(Instant.now());
        }

        jobRepository.save(current);

        // centralize metric increments on actual transitions (based on DB previous state)
        if (previous != JobStatus.RUNNING && status == JobStatus.RUNNING && claimedCounter != null) {
            claimedCounter.increment();
        }
        // only count a completion when the job moved from RUNNING -> COMPLETED
        if (previous == JobStatus.RUNNING && status == JobStatus.COMPLETED && completedCounter != null) {
            completedCounter.increment();
        }
    }

    @Override
    @Transactional
    public Optional<Job> claimNextDueJob() {
        long start = System.nanoTime();
        try {
            Optional<Job> opt = jobRepository.findTopByStatusAndScheduledAtBeforeOrderByScheduledAtAsc(JobStatus.PENDING, Instant.now());
            if (opt.isEmpty()) {
                logger.debug("claimNextDueJob: no candidate found");
                return Optional.empty();
            }

            Job job = opt.get();
            logger.info("claimNextDueJob: found job {}, claiming...", job.getId());

            try {
                String host = InetAddress.getLocalHost().getHostName();
                job.setClaimedBy(host + "-" + UUID.randomUUID());
            } catch (Exception e) {
                job.setClaimedBy("scheduler-" + UUID.randomUUID());
            }

            // use markStatus so the RUNNING transition and metric increment are centralized
            markStatus(job, JobStatus.RUNNING);
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
        } finally {
            long elapsed = System.nanoTime() - start;
            if (claimTimer != null) claimTimer.record(elapsed, TimeUnit.NANOSECONDS);
        }
    }
}
