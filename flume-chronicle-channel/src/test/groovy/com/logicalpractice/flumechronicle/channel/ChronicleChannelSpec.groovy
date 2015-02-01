/*
 * Copyright 2015 Gareth Davis
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.logicalpractice.flumechronicle.channel;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files
import org.apache.flume.Channel
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder
import org.apache.flume.lifecycle.LifecycleState
import spock.lang.Specification

import java.util.concurrent.*

public class ChronicleChannelSpec extends Specification implements ChannelTransactionSupport {

    ChronicleChannel testObject;

    ExecutorService executor = Executors.newCachedThreadPool();

    def setup() throws Exception {
        File tempDir = Files.createTempDir();
        testObject = new ChronicleChannel(name:'chronicle-channel');
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

    def "Put increments the committed size"() throws Exception {
        given:
        Event input = EventBuilder.withBody("This is a Message", Charsets.UTF_8,
                ImmutableMap.of("header1", "value1", "header2", "value2"));

        testObject.start();

        when:
        // perform write
        begin();
        testObject.put(input);
        commitAndClose();

        then:
        testObject.committedSize == 1
    }

    def "Take decrements the committed size"() throws Exception {
        given:
        Event input = EventBuilder.withBody("This is a Message", Charsets.UTF_8,
                ImmutableMap.of("header1", "value1", "header2", "value2"));

        testObject.start();
        // perform write
        begin();
        testObject.put(input);
        commitAndClose();

        expect:
        testObject.committedSize == 1

        when:
        // perform read
        begin();
        Event result = testObject.take();
        commitAndClose();

        then:
        testObject.committedSize == 0
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

    def "take then rollback, can be read on next take"() {
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

    def "take adjusts the position on commit"() {
        given:
        testObject.start()
        def event = EventBuilder.withBody("should be rolled back".bytes)
        begin()
        10.times { testObject.put(event) }
        commitAndClose()

        begin()
        testObject.take()
        commitAndClose()

        expect:
        testObject.position.get() != 0

        when: "read another record"
        def savedPosition = testObject.position.get()
        begin()
        testObject.take()
        commitAndClose()

        then:
        testObject.position.get() == savedPosition + 1
    }

    def "take adjusts the position on taking many"() {
        given:
        testObject.start()
        def event = EventBuilder.withBody("should be rolled back".bytes)
        begin()
        10.times { testObject.put(event) }
        commitAndClose()

        begin()
        testObject.take()
        commitAndClose()

        expect:
        testObject.position.get() != 0

        when: "read another record"
        def savedPosition = testObject.position.get()
        begin()
        5.times { testObject.take() }
        commitAndClose()

        then:
        testObject.position.get() == savedPosition + 5
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

    void begin() {
        begin(testObject)
    }
    void commitAndClose() {
        commitAndClose(testObject)
    }

    void rollbackAndClose() {
        rollbackAndClose(testObject)
    }

}
