/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.logicalpractice.flume.channel.chronicle;

import com.google.common.annotations.VisibleForTesting;
import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.Chronicle;
import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.chronicle.ExcerptAppender;
import net.openhft.chronicle.ExcerptTailer;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Flume NG Channel implementation backed by the OpenHFT Chronicle Queue.
 */
public class ChronicleChannel extends BasicChannelSemantics {
    private static Logger LOGGER = LoggerFactory.getLogger(ChronicleChannel.class);

    // settings
    private String path;

    // internals
    private Chronicle chronicle;

    private ChroniclePosition position;

    @Override
    public void configure(Context context) {
        super.configure(context);

        path = context.getString(ChronicleChannelConfiguration.PATH_KEY);
    }

    @Override
    public synchronized void start() {
        try {
            chronicle = ChronicleQueueBuilder
                    .vanilla(path)
                    .cycleFormat("yyyyMMDDHH")
                    .cycleLength((int) TimeUnit.HOURS.toMillis(1))
                    .build();
        } catch (IOException e) {
            throw new ChannelException("Failed to start Chronicle instance", e);
        }

        position = new ChroniclePosition(path);
        LOGGER.info("ChronicleChannel started, using path {}", path);
        super.start();
    }

    @Override
    public synchronized void stop() {
        try {
            chronicle.close();
        } catch (IOException e) {
            throw new ChannelException("Unable to close the chronicle instance", e);
        }
        super.stop();
    }

    @Override
    protected BasicTransactionSemantics createTransaction() {
        return new ChronicleChannelTransaction(chronicle, position);
    }

    @VisibleForTesting
    Chronicle getChronicle() {
        return chronicle;
    }

    @VisibleForTesting
    ChroniclePosition getPosition() {
        return position;
    }

    enum TransactionType {
        PUT, TAKE, NONE
    }

    private static class ChronicleChannelTransaction extends BasicTransactionSemantics {
        private final Chronicle chronicle;
        private final ChroniclePosition position;

        private TransactionType type = TransactionType.NONE;
        private ExcerptAppender appender;
        private List<Event> appendEvents = new ArrayList<Event>();

        private ExcerptTailer tailer;
        private List<Long> takenIndexes = new ArrayList<Long>();

        public ChronicleChannelTransaction(Chronicle chronicle, ChroniclePosition position) {
            this.chronicle = chronicle;
            this.position = position;
        }

        @Override
        protected void doPut(Event event) throws InterruptedException {
            becomeTransactionType(TransactionType.PUT);
            if (appender == null) {
                try {
                    appender = chronicle.createAppender();
                } catch (IOException e) {
                    throw new ChannelException("unable to create new Appender", e);
                }
            }
            appendEvents.add(event);
        }

        @Override
        protected Event doTake() throws InterruptedException {
            becomeTransactionType(TransactionType.TAKE);
            initialiseTailerIfRequired();

            int threadId = AffinitySupport.getThreadId();
            while (tailer.nextIndex()) {
                if (tailer.compareAndSwapInt(0L, 0, threadId)) {
                    tailer.position(4); // skip the lock field
                    takenIndexes.add(tailer.index());
                    return EventBytes.readFrom(tailer);
                }
            }
            return null;
        }

        private void initialiseTailerIfRequired() {
            if (tailer == null) {
                try {
                    tailer = chronicle.createTailer();
                    tailer.index(position.get()); // fast forward to last known safe position
                } catch (IOException e) {
                    throw new ChannelException("unable to create new Tailer", e);
                }
            }
        }

        @Override
        protected void doCommit() throws InterruptedException {
            switch (type) {
                case PUT:
                    doPutCommit();
                    break;
                case TAKE:
                    doTakeCommit();
                    break;
            }
        }

        private void doTakeCommit() {
            long current, update;
            do {
                current = position.get();
                update = current;
                for (Long index : takenIndexes) {
                    if (sequentialIndexFrom(update, index)) {
                        update = index;
                    }
                }
            } while (!position.compareAndSwap(current, update));
        }

        private void becomeTransactionType(TransactionType newType) {
            if (type == TransactionType.NONE) {
                type = newType;
            } else if (type != newType) {
                throw new IllegalStateException("Attempt to switch a " + type + " transaction into a " + newType);
            }
        }

        private boolean sequentialIndexFrom(long from, long to) {
            // todo complete this sequentialIndexFrom function...
            return true;
        }

        private void doPutCommit() {
            for (Event event : appendEvents) {
                appender.startExcerpt();
                appender.writeInt(0); //
                EventBytes.writeTo(appender, event);
                appender.finish();
            }
        }

        @Override
        protected void doRollback() throws InterruptedException {
            throw new UnsupportedOperationException("not implemented yet");
        }
    }
}
