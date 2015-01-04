package com.logicalpractice.flume.channel.chronicle.tools

import com.logicalpractice.flume.channel.chronicle.ChronicleChannel
import com.logicalpractice.flume.channel.chronicle.ChronicleChannelConfiguration
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
