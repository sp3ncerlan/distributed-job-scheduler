// java
package com.spencer.distributed_job_scheduler.executor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spencer.distributed_job_scheduler.model.Job;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.Iterator;

@Component
public class HttpJobExecutor implements JobExecutor {

    private final RestTemplate restTemplate;
    private final ObjectMapper objectMapper;

    public HttpJobExecutor() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setConnectTimeout(10_000);
        requestFactory.setReadTimeout(10_000);

        this.restTemplate = new RestTemplate(requestFactory);
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public String getJobType() {
        return "HTTP";
    }

    @Override
    public void execute(Job job) throws Exception {
        JsonNode config = objectMapper.readTree(job.getPayload());

        String url = config.path("url").textValue();
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("Missing or empty `url` in job payload");
        }

        String method = config.path("method").textValue();
        if (method == null || method.isEmpty()) {
            method = "POST";
        }

        String body = null;
        JsonNode bodyNode = config.get("body");
        if (bodyNode != null && !bodyNode.isNull()) {
            body = bodyNode.isTextual() ? bodyNode.textValue() : bodyNode.toString();
        }

        HttpHeaders headers = new HttpHeaders();
        JsonNode headersNode = config.get("headers");
        if (headersNode != null && headersNode.isObject()) {
            Iterator<String> names = headersNode.fieldNames();
            while (names.hasNext()) {
                String name = names.next();
                JsonNode valueNode = headersNode.get(name);
                if (valueNode != null && !valueNode.isNull()) {
                    String value = valueNode.isTextual() ? valueNode.textValue() : valueNode.toString();
                    headers.add(name, value);
                }
            }
        }

        HttpMethod httpMethod;
        try {
            httpMethod = HttpMethod.valueOf(method.toUpperCase());
        } catch (IllegalArgumentException ex) {
            httpMethod = HttpMethod.POST;
        }

        HttpEntity<String> entity = new HttpEntity<>(body, headers);
        restTemplate.exchange(url, httpMethod, entity, String.class);
    }
}
