package com.logicalpractice.flumechronicle.channel;

import java.util.Arrays;

/**
 * Simple self resizing long array primitive.
 */
class ResizingLongArray {

    private long[] array;
    private int size = 0;

    public ResizingLongArray(int initialSize) {
        this.array = new long[initialSize];
    }

    public long get(int index) {
        return array[index];
    }

    public void set(int index, long value) {
        array[index] = value;
    }

    public int size() {
        return size;
    }

    public void add(long value) {
        if (size == (array.length - 1)) {
            long[] replacement = new long[array.length * 2];
            System.arraycopy(array, 0, replacement, 0, array.length);
            array = replacement;
        }
        array[size] = value;
        size += 1;
    }

    @Override
    public String toString() {
        return "ResizingLongArray{" +
                "size=" + size +
                ", array=" + Arrays.toString(array) +
                '}';
    }
}
