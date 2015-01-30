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
