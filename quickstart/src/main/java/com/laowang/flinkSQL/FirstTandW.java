package com.laowang.flinkSQL;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * Created by  on 2019/1/16.
 */
public class FirstTandW implements AssignerWithPeriodicWatermarks<Tuple3<Long,String, Long>> {

	private final long maxOutOfOrderness = 3500; // 3.5 seconds

	private long currentMaxTimestamp;
	@Override
	public long extractTimestamp(Tuple3<Long,String, Long> element, long previousElementTimestamp) {
		// TODO Auto-generated method stub
		long timestamp = element.f0;
		currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
		return timestamp;
	}

	@Override
	public Watermark getCurrentWatermark() {
		// TODO Auto-generated method stub
		return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
	}

}