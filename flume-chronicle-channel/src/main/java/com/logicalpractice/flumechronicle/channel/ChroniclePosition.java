package com.logicalpractice.flumechronicle.channel;

import net.openhft.lang.io.VanillaMappedBytes;
import net.openhft.lang.io.VanillaMappedFile;
import org.apache.flume.ChannelException;

import java.io.File;
import java.io.IOException;

public class ChroniclePosition {

    private final VanillaMappedBytes bytes;

    public ChroniclePosition(String path) {
        try {
            File position = new File(path, "position.dat");
            bytes = VanillaMappedFile.readWriteBytes(position, 8);
        } catch (IOException e) {
            throw new ChannelException("Failed to open take position file", e);
        }
    }

    public long get() {
        return bytes.readVolatileLong(0L);
    }

    public boolean compareAndSwap(long expect, long update) {
        return bytes.compareAndSwapLong(0L, expect, update);
    }

    public void set(long update) {
        bytes.writeOrderedLong(0L, update);
    }

    public void close() {
        bytes.close();
    }
}
