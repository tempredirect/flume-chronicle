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
