package com.logicalpractice.flumechronicle.channel;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestChroniclePosition {

    ChroniclePosition testObject;

    @Before
    public void setUp() throws Exception {
        testObject = new ChroniclePosition(Files.createTempDir().getCanonicalPath());
    }

    @After
    public void tearDown() throws Exception {
        testObject.close();
    }

    @Test
    public void test() throws Exception {
        assertEquals(0L, testObject.get());
        assertTrue(testObject.compareAndSwap(0L, 1L));


        assertEquals(1L, testObject.get());

        assertFalse(testObject.compareAndSwap(0L, 3L));
    }
}