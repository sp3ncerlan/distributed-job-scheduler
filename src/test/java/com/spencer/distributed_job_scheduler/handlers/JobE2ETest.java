package com.spencer.distributed_job_scheduler.handlers;

import com.spencer.distributed_job_scheduler.executor.JobExecutor;
import com.spencer.distributed_job_scheduler.model.Job;
import com.spencer.distributed_job_scheduler.model.JobStatus;
import com.spencer.distributed_job_scheduler.repository.JobRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedDeque;

import static org.assertj.core.api.Fail.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
@Import(TestRedisAndExecutorConfig.class)
public class JobE2ETest {

    static final String WORK_QUEUE = "scheduler:work";

    @Autowired
    JobRepository jobRepository;

    @Autowired
    JobExecutor jobExecutor;

    @Autowired
    StringRedisTemplate redisTemplate;


    @BeforeEach
    @SuppressWarnings("unchecked")
    void setupMocks() throws Exception {
        ListOperations<String, String> listOps = Mockito.mock(ListOperations.class);
        ConcurrentLinkedDeque<String> deque = new ConcurrentLinkedDeque<>();

        Mockito.when(redisTemplate.opsForList()).thenReturn(listOps);

        // leftPush -> push to head
        Mockito.when(listOps.leftPush(Mockito.eq(WORK_QUEUE), Mockito.anyString()))
                .thenAnswer(invocation -> {
                    String value = invocation.getArgument(1);
                    deque.addFirst(value);
                    return (long) deque.size();
                });

        // rightPop with timeout -> pop from tail (FIFO)
        Mockito.when(listOps.rightPop(Mockito.eq(WORK_QUEUE), Mockito.any(Duration.class)))
                .thenAnswer(invocation -> deque.pollLast());

        // mock executor to simulate short work
        Mockito.doAnswer(invocation -> {
            try { Thread.sleep(10); } catch (InterruptedException ignored) {}
            return null;
        }).when(jobExecutor).execute(Mockito.any(Job.class));
    }

    @Test
    void workerProcessesQueuedJob() throws Exception {
        Job job = new Job();
        job.setJobType("HTTP");
        job.setStatus(JobStatus.PENDING);
        job.setScheduledAt(Instant.now());
        Job savedJob = jobRepository.save(job);

        // push to mocked work queue
        redisTemplate.opsForList().leftPush(WORK_QUEUE, savedJob.getId().toString());

        // wait 15 seconds for a worker to process
        long deadline = System.currentTimeMillis() + 15_000;
        while (System.currentTimeMillis() < deadline) {
            Optional<Job> finalJob = jobRepository.findById(savedJob.getId());
            if (finalJob.isPresent()) {
                Job retrievedJob = finalJob.get();

                if (retrievedJob.getStatus() == JobStatus.COMPLETED ||
                    retrievedJob.getStatus() == JobStatus.FAILED) {
                    break;
                }
            }

            Thread.sleep(200);
        }

        // assert
        Job processedJob = jobRepository.findById(savedJob.getId())
                .orElseThrow(() -> new IllegalStateException("Job disappeared from repository"));

        if (processedJob.getStatus() != JobStatus.COMPLETED) {
            fail("Expected job to be COMPLETED but was " + processedJob.getStatus());
        }

        assertEquals(JobStatus.COMPLETED, processedJob.getStatus());
    }
}
