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

package com.logicalpractice.flumechronicle.channel

import com.google.common.base.Charsets
import com.google.common.collect.ImmutableMap
import com.google.common.io.Files
import org.apache.flume.Context
import org.apache.flume.event.EventBuilder
import spock.lang.Specification

/**
 *
 */
class ChronicleChannelRecoverSpec extends Specification implements ChannelTransactionSupport {

    File tempDir = Files.createTempDir()

    def "Reopening a channel should preserve it's contents"() {
        given:
        def channel = newChronicleChannel(tempDir)
        begin(channel)
        100.times { channel.put(EventBuilder.withBody("This is a Message", Charsets.UTF_8)) }
        commitAndClose(channel)
        channel.stop()

        channel = newChronicleChannel(tempDir)

        when: "read from the channel"
        begin(channel)
        def events = (1..100).collect { channel.take() }
        def next = channel.take()

        then: "it should have only 100 items"
        events.every { it != null }
        next == null
    }

    def "Reopening a channel should restore it's size"() {
        given:
        def channel = newChronicleChannel(tempDir)
        begin(channel)
        100.times { channel.put(EventBuilder.withBody("This is a Message", Charsets.UTF_8)) }
        commitAndClose(channel)
        channel.stop()

        when: "reopen the channel"
        channel = newChronicleChannel(tempDir)

        then: "it's size is 100"
        channel.committedSize == 100
    }

    def "Reopening a that has partially committed contents should exclude the uncommitted"() {
        given:
        def channel = newChronicleChannel(tempDir)
        begin(channel)
        2.times { channel.put(EventBuilder.withBody("COMMITTED", Charsets.UTF_8)) }
        commitAndClose(channel)
        begin(channel)
        2.times { channel.put(EventBuilder.withBody("SHOULD BE DROPPED", Charsets.UTF_8)) }
        // note no commitAndClose

        channel.stop()

        channel = newChronicleChannel(tempDir)

        when: "read from the channel"
        begin(channel)
        def events = (1..2).collect { channel.take() }
        def next = channel.take()

        then: "it should have only 2 items"
        events.collect { new String(it.getBody(),Charsets.UTF_8)  }.every { it == "COMMITTED" }
        next != EventBuilder.withBody("SHOULD BE DROPPED", Charsets.UTF_8)
    }

    def "Reopening a that has uncommitted takes should restore them"() {
        given:
        def channel = newChronicleChannel(tempDir)
        begin(channel)
        5.times { channel.put(EventBuilder.withBody("Event body", Charsets.UTF_8)) }
        commitAndClose(channel)

        when: "take all but with out a commit"
        begin(channel)
        5.times { channel.take() }
        // note no commitAndClose
        channel.stop()
        channel = newChronicleChannel(tempDir)

        then:
        channel.committedSize == 5L

        when: "read from the channel"
        begin(channel)
        def events = (1..5).collect { channel.take() }

        then: "get 5 events"
        events.collect { new String(it.getBody(),Charsets.UTF_8)  }.every { it == "Event body" }
    }

    def "Reopening with a reset position file"() {
        given:
        def channel = newChronicleChannel(tempDir)
        begin(channel)
        5.times { channel.put(EventBuilder.withBody("Event body", Charsets.UTF_8)) }
        commitAndClose(channel)

        when: "take 3, stop the channel and delete the position.dat"
        begin(channel)
        3.times { channel.take() }
        commitAndClose(channel)
        def previousPosition = channel.position.get()

        channel.stop()

        new File(tempDir, "position.dat").delete()

        channel = newChronicleChannel(tempDir)

        then:
        channel.committedSize == 2L
        channel.position.get() == previousPosition
    }

    ChronicleChannel newChronicleChannel(File path) {
        def result = new ChronicleChannel(name:'chronicle-channel');
        result.configure(new Context(ImmutableMap.of(
                ChronicleChannelConfiguration.PATH_KEY, path.getCanonicalPath()
        )));
        result.start()
        result
    }
}
