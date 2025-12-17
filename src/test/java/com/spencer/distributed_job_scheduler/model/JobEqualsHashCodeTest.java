package com.spencer.distributed_job_scheduler.model;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

public class JobEqualsHashCodeTest {

    private static void setId(Job job, UUID id) throws Exception {
        Field f = Job.class.getDeclaredField("id");
        f.setAccessible(true);
        f.set(job, id);
    }

    @Test
    public void equals_and_hasCode_sameId() throws Exception {
        Job a = new Job();
        Job b = new Job();

        UUID id = UUID.randomUUID();

        setId(a, id);
        setId(b, id);

        assertEquals(a, b);
        assertEquals(b, a);
        assertEquals(a.hashCode(), b.hashCode());
    }

    @Test
    public void not_equal_differentId() throws Exception {
        Job a = new Job();
        Job b = new Job();

        setId(a, UUID.randomUUID());
        setId(b, UUID.randomUUID());

        assertNotEquals(a, b);
        assertNotEquals(b, a);
    }

    @Test
    public void transient_instances_use_identityHash() {
        Job a = new Job();
        Job b = new Job();

        assertNotEquals(a, b);
        assertEquals(System.identityHashCode(a), a.hashCode());
        assertEquals(System.identityHashCode(b), b.hashCode());

        assertNotEquals(a.hashCode(), b.hashCode());
    }
}
