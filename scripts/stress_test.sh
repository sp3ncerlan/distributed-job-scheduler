#!/bin/bash

# 1. The Target (Use httpbin.org to avoid getting banned by Google)
# We use /delay/2 to simulate a job that takes 2 seconds to finish.
payload='{
  "jobType": "HTTP",
  "scheduledAt": null,
  "payload": {
    "url": "https://httpbin.org/delay/2",
    "method": "GET"
  }
}'

echo "🚀 Launching 50 concurrent jobs..."

for i in {1..50}
do
   # The '&' at the end makes it run in the background (Parallel)
   curl -s -X POST http://localhost:8080/jobs \
   -H "Content-Type: application/json" \
   -d "$payload" > /dev/null &
done

echo "✅ All requests fired! Check your application logs."