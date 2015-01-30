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

import net.openhft.lang.io.AbstractBytes;
import net.openhft.lang.io.Bytes;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class EventBytes {

    public static int sizeOf(Event event) {
        int size = 2; // short header
        for (Map.Entry<String, String> entry : event.getHeaders().entrySet()) {
            size += 4; // 2 * 2 length fields
            size += AbstractBytes.findUTFLength(entry.getKey(), entry.getKey().length());
            size += AbstractBytes.findUTFLength(entry.getValue(), entry.getValue().length());
        }
        size += 4;
        size += event.getBody().length;
        return size;
    }

    public static void writeTo(Bytes out, Event event) {
        Map<String, String> headers = event.getHeaders();
        out.writeShort(headers.size());
        if (headers.size() > 0) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeUTF(entry.getValue());
            }
        }
        out.writeInt(event.getBody().length);
        out.write(event.getBody());
    }

    public static Event readFrom(Bytes in) {
        int numberOfHeaders = in.readShort();
        Map<String, String> headers = new HashMap<String, String>();
        for (int i = 0; i < numberOfHeaders; i++) {
            headers.put(in.readUTF(), in.readUTF());
        }
        int bodySize = in.readInt();
        byte[] body = new byte[bodySize];
        int bodyRead = in.read(body);
        assert bodyRead == bodySize;
        return EventBuilder.withBody(body, headers);
    }
}
