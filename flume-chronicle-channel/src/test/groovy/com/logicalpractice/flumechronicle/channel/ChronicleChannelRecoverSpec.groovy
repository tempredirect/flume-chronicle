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

    ChronicleChannel newChronicleChannel(File path) {
        def result = new ChronicleChannel(name:'chronicle-channel');
        result.configure(new Context(ImmutableMap.of(
                ChronicleChannelConfiguration.PATH_KEY, path.getCanonicalPath()
        )));
        result.start()
        result
    }
}
