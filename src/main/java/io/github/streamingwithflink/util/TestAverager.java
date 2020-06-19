package io.github.streamingwithflink.util;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class TestAverager implements WindowFunction<SensorReading, SensorReading, String, TimeWindow> {
    @Override
    public void apply(String sensorId, TimeWindow window, Iterable<SensorReading> input, Collector<SensorReading> out) throws Exception {
        int cnt = 0;
        double sum = 0.0;
        for (SensorReading r : input) {
            cnt++;
            sum += r.temperature;
        }
        double avgTemp = sum / cnt;

        // emit a SensorReading with the average temperature
        out.collect(new SensorReading(sensorId, window.getEnd(), avgTemp));
    }
}
