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
import org.apache.flume.channel.file.FileChannel
import org.apache.flume.channel.file.FileChannelConfiguration
import org.apache.flume.conf.Configurables
import org.apache.flume.event.EventBuilder
import org.apache.log4j.BasicConfigurator

/**
 * Created by gadavis on 04/01/2015.
 */
class PerformanceTool {

    public static void main(String[] args) {
        BasicConfigurator.configure()

        ArgumentParser parser = ArgumentParsers.newArgumentParser("performance-tool")

        parser.addArgument("--channel-type")
                .metavar("type")
                .choices("file", "chronicle")
                .setDefault("chronicle")
                .required(true)

        parser.addArgument("--path")
                .metavar("path")
                .required(true)

        parser.addArgument("--event-count")
                .metavar("amount")
                .setDefault(-1)
                .type(Integer)

        Namespace ns = parser.parseArgsOrFail(args)

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
            default:
                System.out.println("Unknown channel-type: $type")
                System.exit(1)
        }


        FileUtils.deleteDirectory(path)
        path.mkdirs()

        Configurables.configure(channel, channelContext)

        channel.start()

        ChannelLoadDriver driver = new ChannelLoadDriver(
                channel: channel,
                count: ns.getInt("event_count"),
                eventSupplier: {EventBuilder.withBody("Some content".bytes)}
        )
        def start = System.currentTimeMillis()
        try {
            driver.run()
        } finally {
            channel.stop()
        }
        println "finished run"
        println "time taken: ${System.currentTimeMillis() - start}ms"
    }
}
