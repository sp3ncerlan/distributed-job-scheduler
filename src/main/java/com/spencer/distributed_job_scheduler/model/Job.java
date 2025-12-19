package com.spencer.distributed_job_scheduler.model;

import jakarta.persistence.*;
import lombok.*;
import org.hibernate.proxy.HibernateProxy;

import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@ToString(exclude = "payload")
@Entity
@Table(name = "jobs", indexes = {
        @Index(name = "idx_jobs_status_scheduled", columnList = "status, scheduled_at")
})
public class Job {

    @Id
    @Setter(AccessLevel.NONE)
    @Column(columnDefinition = "uuid")
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private JobStatus status;

    @Column(name = "job_type", nullable = false)
    private String jobType;

    @Column(name = "callback_url")
    private String callbackUrl;

    @Column(name = "scheduled_at", nullable = false)
    private Instant scheduledAt;

    @Column(name = "started_at", nullable = false)
    private Instant startedAt;

    @Column(name = "finished_at", nullable = false)
    private Instant finishedAt;

    @Column(columnDefinition = "text")
    private String payload;

    @Version
    @Setter(AccessLevel.NONE)
    private Long version;

    @PrePersist
    private void ensureId() {
        if (this.id == null) {
            this.id = UUID.randomUUID();
        }
    }

    // helper for extracting real class when proxy is involved
    private static Class<?> effectiveClass(Object o) {
        return o instanceof HibernateProxy ? ((HibernateProxy) o).getHibernateLazyInitializer().getPersistentClass() : o.getClass();
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        if (effectiveClass(this) != effectiveClass(o)) return false;
        Job job = (Job) o;
        return getId() != null && Objects.equals(getId(), job.getId());
    }

    @Override
    public final int hashCode() {
        UUID id = getId();
        if (id != null) {
            return Objects.hash(effectiveClass(this), id);
        }
        return System.identityHashCode(this);
    }
}
