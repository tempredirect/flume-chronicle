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
