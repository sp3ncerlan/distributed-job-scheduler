package com.spencer.distributed_job_scheduler.handlers;

import com.spencer.distributed_job_scheduler.executor.JobExecutor;
import com.spencer.distributed_job_scheduler.model.Job;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.redis.core.ListOperations;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.mockito.Mockito;

import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedDeque;

@TestConfiguration
public class TestRedisAndExecutorConfig {

    private static final String WORK_QUEUE = "scheduler:work";

    @Bean
    public StringRedisTemplate stringRedisTemplate() {
        StringRedisTemplate mockRedis = Mockito.mock(StringRedisTemplate.class);
        @SuppressWarnings("unchecked")
        ListOperations<String, String> listOps = Mockito.mock(ListOperations.class);

        ConcurrentLinkedDeque<String> deque = new ConcurrentLinkedDeque<>();

        Mockito.when(mockRedis.opsForList()).thenReturn(listOps);

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

        return mockRedis;
    }

    @Bean
    public JobExecutor jobExecutor() {
        JobExecutor mockExecutor = Mockito.mock(JobExecutor.class);

        try {
            Mockito.doAnswer(invocation -> {
                // simulate work
                try {
                    Thread.sleep(10);
                } catch (InterruptedException ignored) {}

                return null;
            }).when(mockExecutor).execute(Mockito.any(Job.class));
        } catch (Exception ignored) {

        }

        return mockExecutor;
    }
}
