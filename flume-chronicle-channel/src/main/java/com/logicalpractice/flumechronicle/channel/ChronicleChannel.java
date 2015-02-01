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

package com.logicalpractice.flumechronicle.channel;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import net.openhft.affinity.AffinitySupport;
import net.openhft.chronicle.*;
import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.channel.BasicChannelSemantics;
import org.apache.flume.channel.BasicTransactionSemantics;
import org.mortbay.thread.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Flume NG Channel implementation backed by the OpenHFT Chronicle Queue.
 *
 * The channel protocol is controlled by a 4 byte header on each record.
 *
 * Excerpt:
 *    4-byte control
 *    Encoded FlumeEvent
 *
 * Control field values:
 *   on put => - thread id
 *   on commit => 0
 *
 *   on take => swapped for the consuming thread id
 *   on commit => set to Integer.MAX_VALUE (0x7fffffff)
 *
 * The 'position' is a container for the last index for which there are no un-taken
 * events. It is maintained on committing takes and shuffles forward as the channel
 * is consumed.
 */
public class ChronicleChannel extends BasicChannelSemantics {
    private static Logger LOGGER = LoggerFactory.getLogger(ChronicleChannel.class);

    // settings
    private String path;

    // internals
    private Chronicle chronicle;

    private ChroniclePosition position;

    private AtomicLong committedSize = new AtomicLong(0L);

    private ScheduledExecutorService scheduledExecutorService;

    private ScheduledFuture<?> cleanupFuture;

    @Override
    public void configure(Context context) {
        super.configure(context);

        path = context.getString(ChronicleChannelConfiguration.PATH_KEY);
    }

    @Override
    public synchronized void start() {
        ChronicleQueueBuilder.VanillaChronicleQueueBuilder queueBuilder;
        try {
            queueBuilder = ChronicleQueueBuilder
                    .vanilla(path)
                    .cycleFormat("yyyyMMDDHH")
                    .cycleLength((int) TimeUnit.HOURS.toMillis(1));

            chronicle = queueBuilder.build();
        } catch (IOException e) {
            throw new ChannelException("Failed to start Chronicle instance", e);
        }

        position = new ChroniclePosition(path);

        performRecovery();
        LOGGER.info("{} started, using path {}", getName(), path);

        ThreadFactory threadFactory = new ThreadFactoryBuilder()
                .setDaemon(true)
                .setNameFormat(getName() + "-cleanup-%s")
                .build();

        scheduledExecutorService = Executors.newScheduledThreadPool(1, threadFactory);
        cleanupFuture = scheduledExecutorService.scheduleAtFixedRate(
                new ChronicleCleanup(queueBuilder, new File(path), position),
                1, 30, TimeUnit.MINUTES);

        super.start();
    }

    private void performRecovery() throws ChannelException {
        // starting at the initial position read forward, finding any records that
        //  - where mid put - these should be discarded (ie flagged as taken)
        //  - taken but not committed - these should be reset
        // if we find a committed take (control == MAX_VALUE) and we are sequential from
        // the current position, we must advance the position.
        long putsDiscarded = 0, takesRecovered = 0, size = 0;
        try (ExcerptTailer tailer = chronicle.createTailer()) {
            long lastPosition = position.get();
            tailer.index(lastPosition);
            while (tailer.nextIndex()) {
                long currentPosition = tailer.index();
                int control = tailer.readInt(0L);
                if (control == 0) {
                    size += 1;
                } else
                if (control == Integer.MAX_VALUE && sequentialIndexFrom(chronicle, lastPosition, currentPosition)) {
                    lastPosition = currentPosition;
                } else
                if (control < 0 && control > Integer.MIN_VALUE) {
                    // is an un-committed put
                    tailer.writeOrderedInt(0L, Integer.MAX_VALUE);
                    putsDiscarded += 1;
                } else
                if (control > 0 && control < Integer.MAX_VALUE) {
                    // is an un-committed take, put it back
                    tailer.writeOrderedInt(0L, 0);
                    takesRecovered += 1;
                    size += 1;
                }
            }
            position.set(lastPosition);
        } catch (IOException e) {
            throw new ChannelException("Failed recovery", e);
        }
        LOGGER.info("recovery complete: committedSize={}, discarded={}, recovered={}",
                size, putsDiscarded, takesRecovered);
        committedSize.set(size);
    }

    @Override
    public synchronized void stop() {
        try {
            chronicle.close();
        } catch (IOException e) {
            throw new ChannelException("Unable to close the chronicle instance", e);
        } finally {
            if (cleanupFuture != null) {
                cleanupFuture.cancel(false);
                scheduledExecutorService.shutdown(); // it'll shutdown eventually
            }
        }

        super.stop();
        LOGGER.info("{} stopped", getName());
    }

    @Override
    protected BasicTransactionSemantics createTransaction() {
        return new ChronicleChannelTransaction(chronicle, position, committedSize);
    }

    /**
     * The number of events that have been put and committed, but not taken.
     * @return non negative long
     */
    public long getCommittedSize() {
        return committedSize.get();
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

        private final ResizingLongArray indexes = new ResizingLongArray(64);

        private final Chronicle chronicle;

        private final ChroniclePosition position;
        private final AtomicLong committedSize;

        private TransactionType type = TransactionType.NONE;

        // note that the chronicle keeps WeakReference to the appender & tailer instances
        // but in order to ensure that these don't get collected during a transaction we
        // keep a hard reference to them
        private ExcerptAppender appender;
        private ExcerptTailer tailer;

        public ChronicleChannelTransaction(Chronicle chronicle, ChroniclePosition position, AtomicLong committedSize) {
            this.chronicle = chronicle;
            this.position = position;
            this.committedSize = committedSize;
        }

        @Override
        protected void doPut(Event event) throws InterruptedException {
            becomeTransactionType(TransactionType.PUT);
            initialiseAppenderIfRequired();

            appender.startExcerpt(4 + EventBytes.sizeOf(event));

            appender.writeInt(-AffinitySupport.getThreadId()); // initial value for the lock is a neg number
                                                               // this will be zero'd in the commit phase
            EventBytes.writeTo(appender, event);
            appender.finish();
            indexes.add(appender.lastWrittenIndex());
        }

        @Override
        protected Event doTake() throws InterruptedException {
            becomeTransactionType(TransactionType.TAKE);
            initialiseTailerIfRequired();

            int threadId = AffinitySupport.getThreadId();
            long firstPos = tailer.index(); // could be 0
            long scans = 0L;

            // first loop... from current pos until end
            // hopefully this is the normal fast case.
            while (tailer.nextIndex()) {
                scans += 1;
                if (acquireRecord(threadId)) {
                    return EventBytes.readFrom(tailer);
                }
            }

            // else from position to firstPos.. this is the exhaustive step
            // to make sure we scan all possible locations
            for(
                toIndex(position.get()); /* reset to earliest available */
                tailer.index() < firstPos; /* while still before firstPos */
                tailer.nextIndex()
            ) {
                scans += 1;
                if (acquireRecord(threadId)) {
                    return EventBytes.readFrom(tailer);
                }
            }
            // nope .. reset the tailer to position, ready for next time
            toIndex(position.get());

            if (LOGGER.isTraceEnabled())
                LOGGER.trace("doTake() - null, threadId={}, firstPos={}, position={}, scans={}, committedSize={}",
                        threadId, firstPos, position.get(), scans, committedSize.get());
            return null;
        }

        private boolean acquireRecord(int threadId) {
            if (tailer.compareAndSwapInt(0L, 0, threadId)) {
                if (LOGGER.isTraceEnabled())
                    LOGGER.trace("doTake() - {}, threadId={}", tailer.index(), threadId);
                indexes.add(tailer.index());
                tailer.position(4); // skip the control field ready for the read of payload
                return true;
            }
            return false;
        }

        private void initialiseTailerIfRequired() {
            if (tailer == null) {
                try {
                    tailer = chronicle.createTailer();
                    toIndex(position.get()); // fast forward to last known safe position
                } catch (IOException e) {
                    throw new ChannelException("unable to create new Tailer", e);
                }
            }
        }

        private void toIndex(long index) {
            if (index > 0) {
                boolean success = tailer.index(index);
                if (!success) {
                    throw new ChannelException("Unable to navigate to " + index);
                }
            } else {
                tailer.toStart();
            }
        }

        private void initialiseAppenderIfRequired() {
            if (appender == null) {
                try {
                    appender = chronicle.createAppender();
                } catch (IOException e) {
                    throw new ChannelException("unable to create new Appender", e);
                }
            }
        }

        @Override
        protected synchronized void doCommit() throws InterruptedException {
            switch (type) {
                case PUT:
                    makeIndexesVisibleToTake();
                    committedSize.addAndGet(indexes.size());
                    break;
                case TAKE:
                    long currentPosition = position.get();
                    makeIndexesFlaggedAsConsumed();
                    committedSize.addAndGet(-indexes.size());
                    LOGGER.debug("takeCommitted pos:{} => {}, size={}", currentPosition, position.get(), committedSize.get());
                    break;
            }
        }

        @Override
        protected synchronized void doRollback() throws InterruptedException {
            // note this is simply the reflection of the doCommit
            switch(type) {
                case PUT:
                    // rolling back puts is exactly the same effect as
                    // them having been consumed by a take ... hence
                    makeIndexesFlaggedAsConsumed();
                    break;
                case TAKE:
                    // undoing a Take is just setting the control int to zero
                    // which is exactly the logic for committing puts
                    makeIndexesVisibleToTake();
                    break;
            }
        }

        private void makeIndexesVisibleToTake() {
            try (ExcerptTailer tailer = chronicle.createTailer()) {
                for (int i = 0; i < indexes.size(); i ++) {
                    long index = indexes.get(i);
                    if (tailer.index(index)) {
                        tailer.writeOrderedInt(0L, 0);
                    }
                }
            } catch (IOException e) {
                throw new ChannelException("unable to initialise a Tailer", e);
            }
        }

        private void makeIndexesFlaggedAsConsumed() {
            long current, update;
            do {
                update = current = position.get();

                for (int i = 0; i < indexes.size(); i++) {
                    long index = indexes.get(i);
                    if (sequentialIndexFrom(chronicle, update, index)) {
                        update = index;
                    } else {
                        if (LOGGER.isTraceEnabled())
                            LOGGER.trace("makeIndexesFlaggedAsConsumed not sequential update pos={}, index={}",
                                    update, index);
                    }
                    toIndex(index); // move to the record then, flag it as consumed
                    tailer.writeOrderedInt(0L, Integer.MAX_VALUE);
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
    }

    private static boolean sequentialIndexFrom(Chronicle chronicle, long from, long to) {
        if ((from + 1) == to) {
            return true;
        }
        try (Excerpt tmpTailer = chronicle.createExcerpt()) {
            if (tmpTailer.index(from)) {
                // only if 'from' is valid and the nextIndex is
                // the 'to'
                return tmpTailer.nextIndex() && tmpTailer.index() == to;
            }
            LOGGER.info("first element consumed?");
            // from isn't valid then 'to' could be the first element?
            tmpTailer.toStart();
            return tmpTailer.nextIndex() && tmpTailer.index() == to;
        } catch (IOException e) {
            throw new ChannelException("unable to create tmp tailer", e);
        }
    }
}
