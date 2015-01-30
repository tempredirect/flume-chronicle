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

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestChroniclePosition {

    ChroniclePosition testObject;

    @Before
    public void setUp() throws Exception {
        testObject = new ChroniclePosition(Files.createTempDir().getCanonicalPath());
    }

    @After
    public void tearDown() throws Exception {
        testObject.close();
    }

    @Test
    public void test() throws Exception {
        assertEquals(0L, testObject.get());
        assertTrue(testObject.compareAndSwap(0L, 1L));


        assertEquals(1L, testObject.get());

        assertFalse(testObject.compareAndSwap(0L, 3L));
    }
}
