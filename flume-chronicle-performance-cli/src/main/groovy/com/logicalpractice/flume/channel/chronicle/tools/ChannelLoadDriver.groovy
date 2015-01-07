package com.logicalpractice.flume.channel.chronicle.tools

import com.google.common.base.Supplier
import groovy.transform.CompileStatic
import org.apache.flume.Channel
import org.apache.flume.Event

/**
 */
@CompileStatic
class ChannelLoadDriver implements Runnable {
    Channel channel
    int count
    Supplier<Event> eventSupplier

    @Override
    void run() {
        if (count > 0) {
            count.times {
                channel.getTransaction().begin()
                channel.put(eventSupplier.get())
                channel.getTransaction().commit()
                channel.getTransaction().close()
            }
        }
    }
}
