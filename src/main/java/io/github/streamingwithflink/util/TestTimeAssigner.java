package io.github.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TestTimeAssigner implements AssignerWithPeriodicWatermarks<SensorReading> {
    private long bound = 10 * 1000;
    private long maxTS = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(maxTS - bound);
    }

    @Override
    public long extractTimestamp(SensorReading element, long previousElementTimestamp) {
        maxTS = Math.max(maxTS, element.timestamp);
        return element.timestamp;
    }
}
