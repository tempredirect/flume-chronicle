package com.logicalpractice.flume.channel.chronicle;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
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
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class TestChronicleChannel {

    ChronicleChannel testObject;

    ExecutorService executor = Executors.newCachedThreadPool();

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
        executor.shutdownNow();
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

    @Test
    public void testEventsInOpenTransactionShouldNotBeVisible() throws Exception {
        testObject.start();
        checkCompleted(executor.invokeAll(Arrays.<Callable<Void>>asList(
                new Put(EventBuilder.withBody("was not committed", Charsets.UTF_8)),
                new PutWithCommit(EventBuilder.withBody("committed", Charsets.UTF_8))
        )));

        begin();
        Event next = testObject.take();
        assertNotNull("Unexpected null event, the channel should have one event available", next);
        assertEquals("Unexpected event message, only the 'committed' event should be visible",
                "committed", new String(next.getBody(), Charsets.UTF_8));

        next = testObject.take();
        assertNull("Unexpected event, the channel should now only have one event visible", next);

        commitAndClose();

        begin();
        next = testObject.take();
        assertNull("Unexpected event, the channel should now only have one event visible", next);
        commitAndClose();
    }

    private void commitAndClose() {
        testObject.getTransaction().commit();
        testObject.getTransaction().close();
    }

    private void begin() {
        testObject.getTransaction().begin();
    }

    private void checkCompleted(List<Future<Void>> futures) {
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (InterruptedException e) {
                throw new AssertionError(e);
            } catch (ExecutionException e) {
                throw Throwables.propagate(e.getCause());
            }
        }
    }


    private class Put implements Callable<Void> {
        private final Event event;

        public Put(Event event) {
            this.event = event;
        }

        @Override
        public Void call() {
            begin();
            testObject.put(event);
            return null;
        }
    }

    public class PutWithCommit extends Put {
        public PutWithCommit(Event event) {
            super(event);
        }

        @Override
        public Void call() {
            super.call();
            commitAndClose();
            return null;
        }
    }


}