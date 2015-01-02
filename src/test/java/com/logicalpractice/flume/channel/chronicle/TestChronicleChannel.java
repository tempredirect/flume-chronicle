package com.logicalpractice.flume.channel.chronicle;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.logicalpractice.flume.channel.chronicle.ChronicleChannel;
import com.logicalpractice.flume.channel.chronicle.ChronicleChannelConfiguration;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class TestChronicleChannel {

    ChronicleChannel testObject;

    @Before
    public void setUp() throws Exception {
        File tempDir = Files.createTempDir();
        testObject = new ChronicleChannel();
        testObject.configure(new Context(ImmutableMap.of(
                ChronicleChannelConfiguration.PATH_KEY, tempDir.getCanonicalPath()
        )));
    }

    @After
    public void tearDown() throws Exception {
        if (testObject.getLifecycleState() == LifecycleState.START) {
            testObject.stop();
        }
    }

    @Test
    public void testStart() throws Exception {
        testObject.start();

        assertEquals("Unexpected lifecycle state after start()",
                LifecycleState.START, testObject.getLifecycleState());
    }

    @Test
    public void testPutAndTakeSingleEvent() throws Exception {
        Event input = EventBuilder.withBody("This is a Message", Charsets.UTF_8,
                ImmutableMap.of("header1", "value1", "header2", "value2"));

        testObject.start();

        // perform write
        begin();
        testObject.put(input);
        commitAndClose();

        // perform read
        begin();
        Event result = testObject.take();
        commitAndClose();

        // check the results
        assertNotNull("Unexpected null event from the channel:", result);
        assertEquals("Unexpected difference between the event headers written and read",
                input.getHeaders(),
                result.getHeaders());

        assertArrayEquals("Unexpected difference between the event body written and read",
                input.getBody(),
                result.getBody());
    }

    @Test
    public void testPutAndTakeMultipleTransactions() throws Exception {

        testObject.start();
        long initial = testObject.getPosition().get();
        // perform writes
        for (int i = 0; i < 10; i++) {
            begin();
            for (int j = 0; j < 5; j++) {
                Event input = EventBuilder.withBody("Message:" + i, Charsets.UTF_8,
                        ImmutableMap.of("txn", String.valueOf(i), "msg", String.valueOf(j)));
                testObject.put(input);
            }
            commitAndClose();
        }

        // perform a single read
        begin();
        testObject.take();
        commitAndClose();

        assertTrue(initial != testObject.getPosition().get());
        initial = testObject.getPosition().get();

        // read another
        begin();
        testObject.take();
        commitAndClose();

        assertEquals(initial + 1, testObject.getPosition().get());
    }

    private void commitAndClose() {
        testObject.getTransaction().commit();
        testObject.getTransaction().close();
    }

    private void begin() {
        testObject.getTransaction().begin();
    }


}