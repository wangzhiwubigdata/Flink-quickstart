package com.laowang.hot;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * Created by  on 2019/1/16.
 */
public class OrderSource implements ParallelSourceFunction {
	@Override
	public void run(SourceContext ctx) throws Exception {

	}

	@Override
	public void cancel() {

	}
}
