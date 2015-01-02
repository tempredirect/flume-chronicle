package com.logicalpractice.flume.channel.chronicle;

import net.openhft.lang.io.Bytes;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class EventBytes {

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
