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

package com.logicalpractice.flumechronicle.performance.cli

import com.logicalpractice.flumechronicle.channel.ChronicleChannel
import com.logicalpractice.flumechronicle.channel.ChronicleChannelConfiguration
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.inf.ArgumentParser
import net.sourceforge.argparse4j.inf.Namespace
import org.apache.commons.io.FileUtils
import org.apache.flume.Channel
import org.apache.flume.Context
import org.apache.flume.channel.MemoryChannel
import org.apache.flume.channel.file.FileChannel
import org.apache.flume.channel.file.FileChannelConfiguration
import org.apache.flume.conf.Configurables
import org.apache.flume.event.EventBuilder
import org.apache.log4j.BasicConfigurator
import org.apache.log4j.Level

import java.util.concurrent.Executors
import org.apache.log4j.Logger;

/**
 *
 */
class PerformanceTool {

    public static void main(String[] args) {
        BasicConfigurator.configure()
        Logger.getRootLogger().setLevel(Level.WARN);
        Logger.getLogger(PerformanceTool.package.name).setLevel(Level.INFO)

        ArgumentParser parser = ArgumentParsers.newArgumentParser("performance-tool")

        parser.addArgument("--channel-type")
                .metavar("type")
                .choices("file", "chronicle", "memory")
                .setDefault("chronicle")

        parser.addArgument("--path")
                .metavar("path")
                .required(true)

        parser.addArgument("--event-count")
                .metavar("amount")
                .setDefault(50_000)
                .type(Integer)

        parser.addArgument("--writers")
                .metavar("count")
                .setDefault(1)
                .type(Integer)

        parser.addArgument("--writer-batch-size")
                .metavar("size")
                .setDefault(100)
                .type(Integer)

        parser.addArgument("--readers")
                .metavar("count")
                .setDefault(1)
                .type(Integer)

        parser.addArgument("--reader-batch-size")
                .metavar("size")
                .setDefault(100)
                .type(Integer)

        parser.addArgument("--warm-up")
                .metavar("count")
                .setDefault(1_000)
                .type(Integer)
                .help("number of events to be pushed though before running the test")

        parser.addArgument("--body-size")
                .metavar("bytes")
                .setDefault(1024)
                .help("size in bytes of the event body")

        parser.addArgument("--log-level")
                .metavar("level")
                .choices("trace", "debug", "info", "warn", "error")

        Namespace ns = parser.parseArgsOrFail(args)

        if (ns.getString("log_level")) {
            Logger.getRootLogger().setLevel(Level.toLevel(ns.getString("log_level")))
        }

        Context channelContext = new Context()
        Channel channel

        def type = ns.getString("channel_type")

        def path = new File(ns.getString("path"))
        switch(type) {
            case "file":
                channelContext.put(FileChannelConfiguration.DATA_DIRS, new File(path, "data").absolutePath)
                channelContext.put(FileChannelConfiguration.CHECKPOINT_DIR, new File(path, "checkpoint").absolutePath)
                channel = new FileChannel()
                channel.setName("fileChannel")
                break
            case "chronicle":
                channelContext.put(ChronicleChannelConfiguration.PATH_KEY, path.absolutePath)
                channel = new ChronicleChannel()
                channel.setName("chronicleChannel")
                break
            case "memory":
                channelContext.put("capacity", "10000")
                channel = new MemoryChannel()
                channel.setName("memoryChannel")
                break
            default:
                System.out.println("Unknown channel-type: $type")
                System.exit(1)
        }

        def executor = Executors.newCachedThreadPool()

        FileUtils.deleteDirectory(path)
        path.mkdirs()

        Configurables.configure(channel, channelContext)

        channel.start()

        if (ns.getInt("warm_up") > 0) {
            println "starting warmup"
            executor.invokeAll([
                    new WriteLoadDriver(
                            channel: channel,
                            count: ns.getInt("warm_up"),
                            eventSupplier: { EventBuilder.withBody("Some content".bytes) }
                    ),
                    new ReadLoadDriver(
                            channel: channel,
                            count: ns.getInt("warm_up")
                    )
            ]).each { it.get() }

            println "warmup done"
        }

        def tasks = []
        def writers = ns.getInt("writers")
        def readers = ns.getInt("readers")
        def eventCount = ns.getInt("event_count")
        def totalEventCount = eventCount * writers
        def eventsPerReader = totalEventCount / readers
        def readerCounts = [eventsPerReader] * readers
        readerCounts[0] += totalEventCount % readers

        tasks += (1..writers).collect {
            new WriteLoadDriver(
                    channel: channel,
                    count: eventCount,
                    eventSupplier: new EventSupplier(ns.getInt("body_size")),
                    batchSize: ns.getInt("writer_batch_size")
            )}

        tasks += readerCounts.collect {
            new ReadLoadDriver(
                    channel: channel,
                    count: it,
                    batchSize: ns.getInt("reader_batch_size")
            )}
        println "starting run"
        def start = System.currentTimeMillis()
        def end
        try {
            executor.invokeAll(tasks).each { it.get() }
            end = System.currentTimeMillis()
        } finally {
            channel.stop()
        }
        println "finished run"
        println "time taken: ${end - start}ms totalEvents:$totalEventCount"


        executor.shutdownNow()
    }
}
