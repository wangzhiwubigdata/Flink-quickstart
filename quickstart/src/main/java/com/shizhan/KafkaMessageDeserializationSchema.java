package com.shizhan;

import com.shizhan.model.Event;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.util.serialization.KeyedDeserializationSchema;

import java.io.IOException;

public class KafkaMessageDeserializationSchema implements KeyedDeserializationSchema<Event> {

    public Event deserialize(byte[] messageKey, byte[] message, String s, int i, long l) throws IOException {
        String messageStr = new String(message);
        String[] tokens = messageStr.split("_");
        Event e =  (new Event(0, null, null, null,  0,
                0, tokens[0], tokens[1]));
        return e;
    }

    public boolean isEndOfStream(Event event) {
        return false;
    }

    public TypeInformation<Event> getProducedType() {
        return TypeExtractor.getForClass(Event.class);
    }
}
