# 🚀 Distributed Job Scheduler

<div align="center">

![Java](https://img.shields.io/badge/Java-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)
![Spring Boot](https://img.shields.io/badge/Spring_Boot-6DB33F?style=for-the-badge&logo=spring-boot&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-DC382D?style=for-the-badge&logo=redis&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)
![Prometheus](https://img.shields.io/badge/Prometheus-E6522C?style=for-the-badge&logo=prometheus&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F2F4F9?style=for-the-badge&logo=grafana&logoColor=orange)

**A high-concurrency job scheduling system capable of handling distributed locking and delayed execution.**
</div>

---

## 📖 Overview

This is an attempt at a **Distributed System** (popular System Design problem), designed to handle the "Double-Booking" problem in clustered environments. There are a lot of use
cases in modern development where this is applicable - most notably, where third-party API calls need to be scheduled by entities utilizing this service.
With that in mind, I wanted to focus on creating it for those HTTP payloads where GET and POST requests can be scheduled and processed once and only once 
even with distributed servers by just providing the right JSON input.

TLDR: It solves the core challenge: **"If I have multiple worker servers, how do I ensure a job scheduled for 12:00 PM runs exactly once?"**

### Key Features
* **Distributed Locking:** Implemented custom Redis-based locking (Lua scripts) to prevent race conditions.
* **Resilient Scheduling:** Uses a "Poller -> Queue -> Worker" architecture to decouple ingestion from execution.
* **Observability:** Prometheus & Grafana dashboard integration to monitor queue depth and lock contention.
* **Horizontal Scalability:** Stateless worker nodes design allows for infinite horizontal scaling.

---

## 🏗 Architecture

The system uses a **Hybrid Polling/Push** architecture to balance reliability and latency.

```mermaid
graph LR
    User[User API] -- POST /jobs --> DB[(Postgres)]
    Poller[Job Poller] -- 1. Poll Due Jobs --> DB
    Poller -- 2. Push ID --> Redis[(Redis Queue)]
    Worker1[Worker A] -- 3. BLPOP --> Redis
    Worker2[Worker B] -- 3. BLPOP --> Redis
    Worker1 -- 4. Acquire Lock --> RedisLock{Redis Lock}
    RedisLock -- Success --> Worker1
    RedisLock -- Fail --> Worker2
    Worker1 -- 5. Execute --> ExtAPI[External API]
```

---

## 📷 Screenshots of Logger:

- Shows a duplicate job failing due to the same Job ID already having a lock from Redis
<img width="1273" height="280" alt="image" src="https://github.com/user-attachments/assets/d9055c9f-6f9b-41cb-9ae7-7393b3390daa" />

- Grafana Dashboard with Prometheus endpoint (skips duplicate entries, so 10 claimed but only 5 completed)
<img width="1646" height="159" alt="image" src="https://github.com/user-attachments/assets/b5cc2e7b-28df-4873-bae0-d5dd8435d472" />


