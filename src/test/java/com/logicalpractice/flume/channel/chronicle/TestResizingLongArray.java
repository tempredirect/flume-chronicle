package com.logicalpractice.flume.channel.chronicle;

import com.logicalpractice.flume.channel.chronicle.ResizingLongArray;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestResizingLongArray {

    ResizingLongArray testObject = new ResizingLongArray(16);

    @Test
    public void testSetAndGet() throws Exception {
        testObject.set(0, 1L);
        assertEquals(1L, testObject.get(0));
    }

    @Test
    public void testAdd() throws Exception {
        assertEquals(0, testObject.size());
        testObject.add(1L);
        assertEquals(1, testObject.size());
        assertEquals(1L, testObject.get(0));
    }

    @Test
    public void testAddWithResize() throws Exception {
        for (int i = 0; i < 64; i++) {
            testObject.add(i);
        }

        assertEquals(64, testObject.size());

        for (int i = 0; i < 64; i++) {
            assertEquals((long) i, testObject.get(i));
        }
    }
}