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

import com.google.common.io.Files
import net.openhft.chronicle.ChronicleQueueBuilder
import net.openhft.chronicle.ExcerptAppender
import spock.lang.Specification

import java.util.concurrent.TimeUnit

/**
 *
 */
class ChronicleCleanupSpec extends Specification {

    File tempDir = Files.createTempDir()

    ChronicleCleanup testObject;

    def setup() {
        ChronicleQueueBuilder.VanillaChronicleQueueBuilder queueBuilder = ChronicleQueueBuilder
                .vanilla(tempDir)
                .cycleFormat("yyyyMMDDHH")
                .cycleLength((int) TimeUnit.HOURS.toMillis(1));

        def chronicle = queueBuilder.build();
        def appender = chronicle.createAppender()
        10.times {
            appender.startExcerpt()
            appender.writeBytes("Message")
            appender.finish()
        }

        def tailer = chronicle.createTailer()
        tailer.toStart()

        def position = new ChroniclePosition(tempDir.getAbsolutePath())
        position.set(tailer.index())

        testObject = new ChronicleCleanup(queueBuilder, tempDir, position)
    }

    def "cleanup should leave the initial contents in place"() {
        when:
        testObject.run()

        then:
        new File(tempDir, "position.dat").exists()
        def otherFiles = tempDir.list { file, name -> name != "position.dat" } as List
        otherFiles.size() == 1
    }

    def "should remove an old directory"() {
        given:
        def directory = new File(tempDir, "2014010114")
        directory.mkdir()
        def index = new File(directory, "index-0")
        index.text = "some content"
        def data = new File(directory, "datafile-1")
        data.text = "some other content"

        expect:
        directory.exists()

        when:
        testObject.run()

        then:
        !index.exists()
        !data.exists()
        !directory.exists()

    }
}
