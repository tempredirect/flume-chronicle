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

import net.openhft.chronicle.ChronicleQueueBuilder;
import net.openhft.lang.Maths;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;

import static java.lang.Character.isDigit;

/**
 * Simple runnable task that sweeps the chronicle directory deleting any directories that
 * are no longer reachable.
 */
public class ChronicleCleanup implements Runnable {
    private final Logger logger = LoggerFactory.getLogger(ChronicleCleanup.class);

    private final ChronicleQueueBuilder.VanillaChronicleQueueBuilder queueBuilder;
    private final File path ;
    private final ChroniclePosition position;

    private final VanillaDateCacheProxy dateCache;

    public ChronicleCleanup(ChronicleQueueBuilder.VanillaChronicleQueueBuilder queueBuilder, File path, ChroniclePosition position) {
        this.queueBuilder = queueBuilder;
        this.path = path;
        this.position = position;

        dateCache = new VanillaDateCacheProxy(queueBuilder);
    }

    @Override
    public void run() {
        long currentPosition = position.get();
        if (currentPosition == 0) {
            return; // no work to do as nothing has been
        }
        // stolen from VanillaChronicle
        int entriesForCycleBits = Maths.intLog2(queueBuilder.entriesPerCycle());
        int cycle = (int) (currentPosition >>> entriesForCycleBits);

        String currentCycleFolder = dateCache.formatFor(cycle);

        File [] directories = path.listFiles(directoriesLessThan(currentCycleFolder));

        for (File directory : directories) {
            logger.info("deleting {}", directory);
            try {
                FileUtils.deleteDirectory(directory);
            } catch (IOException e) {
                logger.warn("unable to remove directory {} - {}", directory, e.toString());
            }
        }
    }

    private FileFilter directoriesLessThan(final String currentFolder) {
        return new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                String name = pathname.getName();
                return pathname.isDirectory()
                        && name.length() == currentFolder.length()
                        && isAllNumeric(name)
                        && name.compareTo(currentFolder) < 0;
            }
        };
    }

    private boolean isAllNumeric(String name) {
        int len = name.length();
        for (int i = 0; i < len; i ++) {
            if (!isDigit(name.charAt(i))) {
                return false;
            }
        }
        return true;
    }
}
