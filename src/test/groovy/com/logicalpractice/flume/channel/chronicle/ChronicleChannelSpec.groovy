package com.logicalpractice.flume.channel.chronicle;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import com.logicalpractice.flume.channel.chronicle.ChronicleChannel;
import com.logicalpractice.flume.channel.chronicle.ChronicleChannelConfiguration;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.lifecycle.LifecycleState;
import org.junit.After;
import org.junit.Before;
import org.junit.Test
import spock.lang.Specification;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class ChronicleChannelSpec extends Specification {

    ChronicleChannel testObject;

    ExecutorService executor = Executors.newCachedThreadPool();

    def setup() throws Exception {
        File tempDir = Files.createTempDir();
        testObject = new ChronicleChannel();
        testObject.configure(new Context(ImmutableMap.of(
                ChronicleChannelConfiguration.PATH_KEY, tempDir.getCanonicalPath()
        )));
    }

    def cleanup() {
        executor.shutdownNow();
        if (testObject.getLifecycleState() == LifecycleState.START) {
            testObject.stop();
        }
    }

    def "start works"() throws Exception {
        when:
        testObject.start();

        then:
        testObject.getLifecycleState() == LifecycleState.START
    }

    def "Put and Take Single Event"() throws Exception {
        given:
        Event input = EventBuilder.withBody("This is a Message", Charsets.UTF_8,
                ImmutableMap.of("header1", "value1", "header2", "value2"));

        testObject.start();

        when:
        // perform write
        begin();
        testObject.put(input);
        commitAndClose();

        // perform read
        begin();
        Event result = testObject.take();
        commitAndClose();

        then:
        // check the results
        result != null
        assertEventsEqual(input, result)
    }

    def "Put And Take Multiple Transactions"() throws Exception {
        given:
        testObject.start();
        long initial = testObject.getPosition().get();

        when:
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

        then:
        initial != testObject.getPosition().get()

        when:
        initial = testObject.getPosition().get();

        // read another
        begin();
        testObject.take();
        commitAndClose();

        then:
        initial + 1 == testObject.getPosition().get()
    }

    def "Events In Open Transaction are not visible"() throws Exception {
        given:
        testObject.start();
        checkCompleted(executor.invokeAll(Arrays.<Callable<Void>>asList(
                new Put(EventBuilder.withBody("was not committed", Charsets.UTF_8)),
                new PutWithCommit(EventBuilder.withBody("committed", Charsets.UTF_8))
        )));

        when:
        begin();
        Event next = testObject.take();

        then: "the channel should have one event available"
        next != null
        and: "only the 'committed' event should be visible"
        "committed" == new String(next.getBody(), Charsets.UTF_8)

        when:
        next = testObject.take();

        then: "the channel should now only have one event visible"

        next == null

        when: "we commit the txn and start again"
        commitAndClose();

        begin();
        next = testObject.take();

        then: "there should still no more events"
        next == null

        cleanup:
        commitAndClose();
    }

    def "put then rollback results in no available event"() {
        given:
        testObject.start()
        def event = EventBuilder.withBody("should be rolled back".bytes)
        checkCompleted(executor.invokeAll([new PutWithCommit(event)]))

        when: "read and rollback"
        begin()
        def result = testObject.take()
        rollbackAndClose()

        then:
        assertEventsEqual(event, result)

        when: "read again"
        begin()
        result = testObject.take()
        rollbackAndClose()

        then: "we get the same event again"
        assertEventsEqual(event, result)
    }

    def "take then rollback and the event is still available"() {

    }

    private void commitAndClose() {
        testObject.getTransaction().commit();
        testObject.getTransaction().close();
    }

    private void rollbackAndClose() {
        testObject.getTransaction().rollback();
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

    private static void assertEventsEqual(Event expected, Event result) {
        assert result != null &&
                expected.getHeaders() == result.getHeaders() &&
                Arrays.equals(expected.getBody(), result.getBody());
    }
}