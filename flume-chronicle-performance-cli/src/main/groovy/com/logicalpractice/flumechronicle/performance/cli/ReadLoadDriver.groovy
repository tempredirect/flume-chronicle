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
import org.apache.flume.Channel;

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
        def event
        channel.getTransaction().begin()
        int batch = 0

        for (int i = 0; i < count; i++) {
            for (;;) {
                event = channel.take()
                if (event != null) {
                    break
                }
                TimeUnit.MILLISECONDS.sleep(1) // back off a wee bit
            }
            received += event.getBody().length
            batch += 1
            if ((batch % batchSize) == 0) {
                channel.getTransaction().commit()
                channel.getTransaction().close()

                channel.getTransaction().begin()
                batch = 0
            }
        }
        channel.getTransaction().commit()
        channel.getTransaction().close()
        log.info "reader finishing"
        received
    }
}
