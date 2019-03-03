package com.shizhan;

import com.shizhan.model.Event;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

public class CustomAssignerWithPeriodicWatermarks implements AssignerWithPeriodicWatermarks<Event> {

    private final long maxOutOfOrderness = 10000; // 10 seconds

    private long currentMaxTimestamp;

    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    public long extractTimestamp(Event event, long kafkaTimestamp) {
        event.timestamp = kafkaTimestamp;
        event.epoch = kafkaTimestamp/1000;
        currentMaxTimestamp = Math.max(kafkaTimestamp, currentMaxTimestamp);
        return kafkaTimestamp;
    }
}