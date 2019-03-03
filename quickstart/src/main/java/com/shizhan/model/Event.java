package com.shizhan.model;

import com.jayway.jsonpath.Configuration;

import java.io.Serializable;

public class Event implements Serializable {

    public long timestamp;
    public String dc;
    public String type;
    public String persona;
    public long epoch;
    public int partition;
    public String key;
    public Object document;

    // Let's stick to a JSON payload for now
    public String payload;

    public Event(long timestamp, String dc, String type, String persona, long epoch,
                 int partition, String key, String payload) {
        this.timestamp = timestamp;
        this.dc = dc;
        this.type = type;
        this.persona = persona;
        this.epoch = epoch;
        this.partition = partition;
        this.key = key;
        this.payload = payload;
        document = Configuration.defaultConfiguration().jsonProvider().parse(payload);
    }

    @Override
    public String toString() {
        return "Event{" +
                "timestamp=" + timestamp +
                ", dc='" + dc + '\'' +
                ", type='" + type + '\'' +
                ", persona='" + persona + '\'' +
                ", epoch=" + epoch +
                ", partition=" + partition +
                ", key='" + key + '\'' +
                ", payload='" + payload + '\'' +
                '}';
    }
}
