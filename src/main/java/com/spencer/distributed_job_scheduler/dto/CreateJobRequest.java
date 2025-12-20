package com.spencer.distributed_job_scheduler.dto;

import jakarta.validation.constraints.NotBlank;
import lombok.Data;

import java.time.Instant;
import java.util.Map;

@Data
public class CreateJobRequest {

    @NotBlank
    private String jobType;

    private Instant scheduledAt;

    private Map<String, Object> payload;
}
