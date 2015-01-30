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

import com.google.common.base.Throwables;
import net.openhft.chronicle.ChronicleQueueBuilder;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Wrapper that uses reflection to access the VanillaDateCache class in chronicle.
 */
class VanillaDateCacheProxy {

    private final Object instance ;

    private final Method formatFor ;

    @SuppressWarnings("unchecked")
    public VanillaDateCacheProxy(ChronicleQueueBuilder.VanillaChronicleQueueBuilder builder) {
        try {
            Class clz = Class.forName("net.openhft.chronicle.VanillaDateCache");
            Constructor c = clz.getConstructor(String.class, int.class);
            c.setAccessible(true);
            instance = c.newInstance(builder.cycleFormat(), builder.cycleLength());
            formatFor = clz.getMethod("formatFor", int.class);
            formatFor.setAccessible(true);
        } catch (ClassNotFoundException
                | NoSuchMethodException
                | InvocationTargetException
                | IllegalAccessException
                | InstantiationException e) {
            throw new RuntimeException(e);
        }
    }

    public String formatFor(int cycle) {
        try {
            return (String) formatFor.invoke(instance, cycle);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw Throwables.propagate(e.getCause());
        }
    }
}
