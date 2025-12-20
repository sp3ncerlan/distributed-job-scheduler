package com.spencer.distributed_job_scheduler.dto;

import lombok.Data;

import java.util.Map;

@Data
public class HttpJobPayload {
    private String url;
    private String method;
    private Map<String, String> headers;
    private Object body;
}
