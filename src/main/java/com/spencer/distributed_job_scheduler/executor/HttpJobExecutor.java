// java
package com.spencer.distributed_job_scheduler.executor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spencer.distributed_job_scheduler.dto.HttpJobPayload;
import com.spencer.distributed_job_scheduler.model.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

@Component
public class HttpJobExecutor implements JobExecutor {

    private static final Logger logger = LoggerFactory.getLogger(HttpJobExecutor.class);

    private final RestClient restClient;
    private final ObjectMapper objectMapper;

    public HttpJobExecutor(RestClient restClient) {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(10_000);
        requestFactory.setReadTimeout(10_000);

        this.restClient = restClient;
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String getJobType() {
        return "HTTP";
    }

    @Override
    public void execute(Job job) throws Exception {
        HttpJobPayload payload = objectMapper.readValue(job.getPayload(), HttpJobPayload.class);

        String method = payload.getMethod() == null ? "GET" : payload.getMethod().toUpperCase();
        var requestSpec = restClient.method(HttpMethod.valueOf(method)).uri(payload.getUrl());

        // set headers from payload safely
        if (payload.getHeaders() != null && !payload.getHeaders().isEmpty()) {
            requestSpec.headers(httpHeaders -> {
                payload.getHeaders().forEach((k, v) -> {
                    if (k == null || v == null) return;                  // skip nulls
                    if ("Content-Length".equalsIgnoreCase(k)            // avoid forbidden headers
                            || "Host".equalsIgnoreCase(k)) return;
                    httpHeaders.add(k, v);
                });
            });
        }

        // body only for non-GET methods; ensure Content-Type when sending JSON
        if (!"GET".equals(method)) {
            if (payload.getBody() != null) {
                // if a client didn't set Content-Type, add application/json
                if (payload.getHeaders() == null || !payload.getHeaders().containsKey("Content-Type")) {
                    requestSpec.header("Content-Type", "application/json");
                }
                requestSpec.body(payload.getBody());
            }
        }

        // execute (API used in earlier conversation)
        requestSpec.retrieve().toBodilessEntity();
    }
}
