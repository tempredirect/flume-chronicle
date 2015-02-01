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

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j;
import org.apache.flume.Channel
import org.apache.flume.Event;

import java.util.concurrent.Callable
import java.util.concurrent.TimeUnit;

/**
 *
 */
@CompileStatic
@Slf4j
class ReadLoadDriver implements Callable<Long> {

    Channel channel
    int count
    int batchSize = 1

    @Override
    public Long call() throws Exception {
        long received = 0L
        channel.getTransaction().begin()

        for (int i = 1; i <= count; i++) {
            Event event = null
            int spinCount = 0;
            while (event == null) {
                event = channel.take()
                spinCount ++
                if (spinCount > 1_000) {
                    log.warn "seem to be stuck waiting for events"
                    spinCount = 0
                }
            }
            received += event.getBody().length
            if ((i % batchSize) == 0) {
                channel.getTransaction().commit()
                channel.getTransaction().close()

                channel.getTransaction().begin()
            }
        }
        channel.getTransaction().commit()
        channel.getTransaction().close()
        log.info "reader finishing"
        received
    }
}
