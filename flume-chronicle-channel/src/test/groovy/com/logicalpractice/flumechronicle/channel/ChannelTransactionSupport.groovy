package com.logicalpractice.flumechronicle.channel

import org.apache.flume.Channel

/**
 *
 */
trait ChannelTransactionSupport {

    void begin(Channel channel) {
        channel.getTransaction().begin();
    }

    void commitAndClose(Channel channel) {
        channel.getTransaction().commit();
        channel.getTransaction().close();
    }


    void rollbackAndClose(Channel channel) {
        channel.getTransaction().rollback();
        channel.getTransaction().close();
    }
}
